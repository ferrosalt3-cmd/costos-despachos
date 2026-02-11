# app.py
# Costos por tarea + Tablero (S/.) — PRO+
# Persistencia GRATIS con Google Sheets (se guarda aunque reinicie Streamlit)

import pandas as pd
import streamlit as st
import altair as alt

from datetime import datetime, date, time, timedelta
from io import BytesIO

from openpyxl import load_workbook
from openpyxl.utils import get_column_letter
from openpyxl.chart import BarChart, Reference
from openpyxl.chart.label import DataLabelList

import gspread
from google.oauth2.service_account import Credentials


# ---------------------------
# Helpers
# ---------------------------

TIPOS_TAREA_DEFAULT = [
    "Movimiento interno",
    "Despacho a carro",
    "Recepción de mercadería",
    "Picking",
    "Verificación de inventario",
    "Emisión de guías",
    "Limpieza y orden",
    "Otros",
]
PRIORIDADES = ["Alta", "Media", "Baja"]
TIPO_PERSONAL = ["Operativo", "Administrativo"]

TABS = ["personal", "equipos", "tareas", "config"]


def safe_float(x, default=0.0) -> float:
    try:
        if pd.isna(x):
            return default
        return float(x)
    except Exception:
        return default


def combine_date_time(d: date, t: time) -> datetime:
    return datetime(d.year, d.month, d.day, t.hour, t.minute, 0)


def hours_between(start_dt: datetime, end_dt: datetime) -> float:
    if end_dt < start_dt:
        end_dt = end_dt + timedelta(days=1)
    return max((end_dt - start_dt).total_seconds() / 3600.0, 0.0)


def money(x: float) -> str:
    return f"S/ {x:,.2f}"


def is_saturday(d: date) -> bool:
    return d.weekday() == 5


def ensure_columns(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c not in df.columns:
            df[c] = None
    return df


def recalc_cost_hora_personal(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["Sueldo_mensual"] = pd.to_numeric(df["Sueldo_mensual"], errors="coerce").fillna(0.0)
    df["Horas_mes"] = pd.to_numeric(df["Horas_mes"], errors="coerce").fillna(0.0)
    df["Costo_hora"] = df.apply(lambda r: (r["Sueldo_mensual"] / r["Horas_mes"]) if r["Horas_mes"] > 0 else 0.0, axis=1)
    return df


def recalc_cost_hora_equipos(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["Costo_mensual"] = pd.to_numeric(df["Costo_mensual"], errors="coerce").fillna(0.0)
    df["Horas_mes"] = pd.to_numeric(df["Horas_mes"], errors="coerce").fillna(0.0)
    df["Costo_hora"] = df.apply(lambda r: (r["Costo_mensual"] / r["Horas_mes"]) if r["Horas_mes"] > 0 else 0.0, axis=1)
    return df


def default_data():
    personal = pd.DataFrame(
        [
            {"Nombre": "Giancarlo Luna", "Tipo": "Operativo", "Sueldo_mensual": 0.0, "Horas_mes": 208, "Activo": True},
            {"Nombre": "Mayra Bejarano", "Tipo": "Operativo", "Sueldo_mensual": 0.0, "Horas_mes": 208, "Activo": True},
            {"Nombre": "Key Mozombite", "Tipo": "Operativo", "Sueldo_mensual": 0.0, "Horas_mes": 208, "Activo": True},
            {"Nombre": "Emiliano Quispe", "Tipo": "Operativo", "Sueldo_mensual": 0.0, "Horas_mes": 208, "Activo": True},
            {"Nombre": "Jose Calvino", "Tipo": "Operativo", "Sueldo_mensual": 0.0, "Horas_mes": 208, "Activo": True},
            {"Nombre": "Miguel Alarcon", "Tipo": "Operativo", "Sueldo_mensual": 0.0, "Horas_mes": 208, "Activo": True},
            {"Nombre": "Cleber Reyes", "Tipo": "Administrativo", "Sueldo_mensual": 0.0, "Horas_mes": 208, "Activo": True},
            {"Nombre": "Jaime Motta", "Tipo": "Administrativo", "Sueldo_mensual": 0.0, "Horas_mes": 208, "Activo": True},
            {"Nombre": "Jose Lopez", "Tipo": "Administrativo", "Sueldo_mensual": 0.0, "Horas_mes": 208, "Activo": True},
        ]
    )
    personal = ensure_columns(personal, ["Costo_hora"])
    personal = recalc_cost_hora_personal(personal)

    equipos = pd.DataFrame(
        [
            {"Codigo": "Apilador 1", "Tipo": "Apilador", "Costo_mensual": 0.0, "Horas_mes": 180, "Activo": True},
            {"Codigo": "Apilador 3", "Tipo": "Apilador", "Costo_mensual": 0.0, "Horas_mes": 180, "Activo": True},
            {"Codigo": "Apilador 4", "Tipo": "Apilador", "Costo_mensual": 0.0, "Horas_mes": 180, "Activo": True},
            {"Codigo": "Montacarga #05", "Tipo": "Montacarga", "Costo_mensual": 0.0, "Horas_mes": 180, "Activo": True},
            {"Codigo": "Montacarga #07", "Tipo": "Montacarga", "Costo_mensual": 0.0, "Horas_mes": 180, "Activo": True},
            {"Codigo": "Montacarga #08", "Tipo": "Montacarga", "Costo_mensual": 0.0, "Horas_mes": 180, "Activo": True},
            {"Codigo": "Montacarga #11", "Tipo": "Montacarga", "Costo_mensual": 0.0, "Horas_mes": 180, "Activo": True},
            {"Codigo": "Transpaleta 3", "Tipo": "Transpaleta", "Costo_mensual": 0.0, "Horas_mes": 200, "Activo": True},
            {"Codigo": "Transpaleta 4", "Tipo": "Transpaleta", "Costo_mensual": 0.0, "Horas_mes": 200, "Activo": True},
        ]
    )
    equipos = ensure_columns(equipos, ["Costo_hora"])
    equipos = recalc_cost_hora_equipos(equipos)

    tareas = pd.DataFrame(
        columns=[
            "ID", "Fecha", "Tipo_tarea", "Prioridad", "Nota", "Programada", "Estado",
            "Inicio", "Fin", "Horas", "Personal_usado", "Equipos_usados",
            "Costo_personal", "Costo_equipos", "Costo_total",
        ]
    )

    config = {
        "horas_lv_efectivas": 8.0,
        "horas_sab_efectivas": 5.5,
        "inicio_lv": "07:30",
        "fin_lv": "16:45",
        "inicio_sab": "07:30",
        "fin_sab": "13:00",
    }

    return {"personal": personal, "equipos": equipos, "tareas": tareas, "config": config}


# ---------------------------
# Google Sheets DB
# ---------------------------

def gs_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
    return gspread.authorize(creds)


def open_sheet():
    return gs_client().open_by_url(st.secrets["GSHEET_URL"])


def ensure_ws(sh, name: str, rows=5000, cols=50):
    try:
        return sh.worksheet(name)
    except Exception:
        return sh.add_worksheet(title=name, rows=str(rows), cols=str(cols))


def ws_to_df(ws) -> pd.DataFrame:
    values = ws.get_all_values()
    if not values:
        return pd.DataFrame()
    header = values[0]
    rows = values[1:]
    if not rows:
        return pd.DataFrame(columns=header)
    return pd.DataFrame(rows, columns=header)


def df_to_ws(ws, df: pd.DataFrame):
    df2 = df.copy()

    # convertir datetimes a texto
    for c in df2.columns:
        if pd.api.types.is_datetime64_any_dtype(df2[c]):
            df2[c] = df2[c].dt.strftime("%Y-%m-%d %H:%M:%S")

    df2 = df2.fillna("")
    values = [df2.columns.tolist()] + df2.astype(str).values.tolist()
    ws.clear()
    ws.update(values)


def save_store(store: dict):
    sh = open_sheet()

    df_to_ws(ensure_ws(sh, "personal"), store["personal"])
    df_to_ws(ensure_ws(sh, "equipos"), store["equipos"])
    df_to_ws(ensure_ws(sh, "tareas"), store["tareas"])

    cfg_df = pd.DataFrame([{"key": k, "value": str(v)} for k, v in store["config"].items()])
    df_to_ws(ensure_ws(sh, "config"), cfg_df)


def load_store() -> dict:
    sh = open_sheet()
    for t in TABS:
        ensure_ws(sh, t)

    store = default_data()

    p = ws_to_df(sh.worksheet("personal"))
    e = ws_to_df(sh.worksheet("equipos"))
    t = ws_to_df(sh.worksheet("tareas"))
    c = ws_to_df(sh.worksheet("config"))

    if not p.empty:
        store["personal"] = p
    if not e.empty:
        store["equipos"] = e
    if not t.empty:
        store["tareas"] = t

    # config
    if not c.empty and {"key", "value"}.issubset(c.columns):
        cfg = dict(zip(c["key"].astype(str), c["value"].astype(str)))
        base = store["config"].copy()

        def to_float(v, d):
            try:
                return float(str(v).replace(",", "."))
            except Exception:
                return d

        base["horas_lv_efectivas"] = to_float(cfg.get("horas_lv_efectivas"), base["horas_lv_efectivas"])
        base["horas_sab_efectivas"] = to_float(cfg.get("horas_sab_efectivas"), base["horas_sab_efectivas"])
        base["inicio_lv"] = cfg.get("inicio_lv", base["inicio_lv"])
        base["fin_lv"] = cfg.get("fin_lv", base["fin_lv"])
        base["inicio_sab"] = cfg.get("inicio_sab", base["inicio_sab"])
        base["fin_sab"] = cfg.get("fin_sab", base["fin_sab"])
        store["config"] = base

    # normalizar tipos
    store["personal"] = ensure_columns(store["personal"], ["Nombre","Tipo","Sueldo_mensual","Horas_mes","Activo","Costo_hora"])
    store["equipos"] = ensure_columns(store["equipos"], ["Codigo","Tipo","Costo_mensual","Horas_mes","Activo","Costo_hora"])
    store["tareas"] = ensure_columns(
        store["tareas"],
        ["ID","Fecha","Tipo_tarea","Prioridad","Nota","Programada","Estado","Inicio","Fin","Horas",
         "Personal_usado","Equipos_usados","Costo_personal","Costo_equipos","Costo_total"]
    )

    # personal/equipos
    p = store["personal"].copy()
    p["Sueldo_mensual"] = pd.to_numeric(p["Sueldo_mensual"], errors="coerce").fillna(0.0)
    p["Horas_mes"] = pd.to_numeric(p["Horas_mes"], errors="coerce").fillna(0.0)
    p["Activo"] = p["Activo"].astype(str).str.lower().isin(["true","1","yes","si","sí"])
    store["personal"] = recalc_cost_hora_personal(p)

    e = store["equipos"].copy()
    e["Costo_mensual"] = pd.to_numeric(e["Costo_mensual"], errors="coerce").fillna(0.0)
    e["Horas_mes"] = pd.to_numeric(e["Horas_mes"], errors="coerce").fillna(0.0)
    e["Activo"] = e["Activo"].astype(str).str.lower().isin(["true","1","yes","si","sí"])
    store["equipos"] = recalc_cost_hora_equipos(e)

    # tareas
    t = store["tareas"].copy()
    t["ID"] = pd.to_numeric(t["ID"], errors="coerce").fillna(0).astype(int)
    t["Programada"] = t["Programada"].astype(str).str.lower().isin(["true","1","yes","si","sí"])
    t["Horas"] = pd.to_numeric(t["Horas"], errors="coerce").fillna(0.0)
    for c in ["Costo_personal", "Costo_equipos", "Costo_total"]:
        t[c] = pd.to_numeric(t[c], errors="coerce").fillna(0.0)

    t["Fecha"] = pd.to_datetime(t["Fecha"], errors="coerce")
    t["Inicio"] = pd.to_datetime(t["Inicio"], errors="coerce")
    t["Fin"] = pd.to_datetime(t["Fin"], errors="coerce")
    store["tareas"] = t

    return store


# ---------------------------
# Charts + Excel export (igual que tu versión)
# ---------------------------

def alt_bar_with_labels(df: pd.DataFrame, x: str, y: str, title: str, horizontal=False):
    if df.empty:
        st.info("Sin datos para este gráfico.")
        return

    base = alt.Chart(df).properties(title=title, height=240)

    if horizontal:
        bars = base.mark_bar().encode(
            y=alt.Y(x, sort="-x", title=None),
            x=alt.X(y, title=None),
            tooltip=[x, y],
        )
        text = base.mark_text(align="left", dx=3).encode(
            y=alt.Y(x, sort="-x"),
            x=alt.X(y),
            text=alt.Text(y),
        )
    else:
        bars = base.mark_bar().encode(
            x=alt.X(x, sort="-y", title=None),
            y=alt.Y(y, title=None),
            tooltip=[x, y],
        )
        text = base.mark_text(dy=-5).encode(
            x=alt.X(x, sort="-y"),
            y=alt.Y(y),
            text=alt.Text(y),
        )

    st.altair_chart((bars + text).interactive(), use_container_width=True)


def export_excel_report(dia: date, tareas_dia: pd.DataFrame, resumen: dict, por_personal: pd.DataFrame, por_tipo: pd.DataFrame) -> bytes:
    output = BytesIO()

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        resumen_df = pd.DataFrame(
            [
                {"Indicador": "Día", "Valor": dia.isoformat()},
                {"Indicador": "Operaciones (total)", "Valor": resumen["ops_total"]},
                {"Indicador": "Finalizadas", "Valor": resumen["finalizadas"]},
                {"Indicador": "Pendientes (no finalizadas)", "Valor": resumen["pendientes"]},
                {"Indicador": "Horas productivas", "Valor": resumen["horas_prod"]},
                {"Indicador": "Horas disponibles", "Valor": resumen["horas_disp"]},
                {"Indicador": "Productividad del turno (%)", "Valor": resumen["prod_pct"]},
                {"Indicador": "Cumplimiento del plan (%)", "Valor": resumen["cumpl_pct"]},
                {"Indicador": "Costo total (finalizadas)", "Valor": resumen["costo_total"]},
            ]
        )
        resumen_df.to_excel(writer, sheet_name="Resumen", index=False)

        detalle = tareas_dia.copy()
        for c in ["Inicio", "Fin"]:
            if c in detalle.columns:
                detalle[c] = detalle[c].astype(str)
        detalle.to_excel(writer, sheet_name="Detalle", index=False)

        por_personal.to_excel(writer, sheet_name="Por_personal", index=False)
        por_tipo.to_excel(writer, sheet_name="Por_tipo", index=False)

    output.seek(0)
    wb = load_workbook(output)

    for ws_name in wb.sheetnames:
        ws = wb[ws_name]
        for col_idx, col in enumerate(ws.iter_cols(values_only=True), start=1):
            max_len = 0
            for v in col:
                if v is None:
                    continue
                max_len = max(max_len, len(str(v)))
            ws.column_dimensions[get_column_letter(col_idx)].width = min(max(max_len + 2, 12), 40)

    out2 = BytesIO()
    wb.save(out2)
    out2.seek(0)
    return out2.getvalue()


# ---------------------------
# App
# ---------------------------

st.set_page_config(page_title="Costos Despachos — PRO+", layout="wide")
st.title("📦 Costos por tarea + Tablero (S/.) — PRO+")

store = load_store()
personal_df = store["personal"]
equipos_df = store["equipos"]
tareas_df = store["tareas"]
config = store["config"]

# Sidebar Config
st.sidebar.header("⚙️ Configuración")

config["horas_lv_efectivas"] = float(st.sidebar.number_input("Horas disponibles L-V (efectivas)", value=float(config.get("horas_lv_efectivas", 8.0)), step=0.25))
config["horas_sab_efectivas"] = float(st.sidebar.number_input("Horas disponibles Sábado (efectivas)", value=float(config.get("horas_sab_efectivas", 5.5)), step=0.25))

st.sidebar.caption("Horario informativo (solo referencia):")
config["inicio_lv"] = st.sidebar.text_input("Inicio L-V", value=str(config.get("inicio_lv", "07:30")))
config["fin_lv"] = st.sidebar.text_input("Fin L-V", value=str(config.get("fin_lv", "16:45")))
config["inicio_sab"] = st.sidebar.text_input("Inicio Sábado", value=str(config.get("inicio_sab", "07:30")))
config["fin_sab"] = st.sidebar.text_input("Fin Sábado", value=str(config.get("fin_sab", "13:00")))

if st.sidebar.button("💾 Guardar configuración"):
    store["config"] = config
    save_store(store)
    st.sidebar.success("Configuración guardada ✅")

tabs = st.tabs(["🗓️ Programar", "⏱️ Ejecutar (Iniciar/Finalizar)", "🧑‍🤝‍🧑 Personal", "🚜 Equipos", "📊 Panel de control"])

# TAB 1: Programar
with tabs[0]:
    st.subheader("🗓️ Programar tareas (plan del día)")
    c1, c2 = st.columns([2, 1])
    with c1:
        tipo_tarea = st.selectbox("Tipo de tarea", TIPOS_TAREA_DEFAULT)
        nota = st.text_input("Nota (opcional)", value="")
    with c2:
        fecha_prog = st.date_input("Fecha programada", value=date.today())
        prioridad = st.selectbox("Prioridad", PRIORIDADES, index=1)

    if st.button("➕ Programar tarea", type="primary"):
        next_id = 1 if tareas_df.empty else int(pd.to_numeric(tareas_df["ID"], errors="coerce").fillna(0).max()) + 1
        new_row = {
            "ID": next_id,
            "Fecha": pd.Timestamp(fecha_prog),
            "Tipo_tarea": tipo_tarea,
            "Prioridad": prioridad,
            "Nota": nota,
            "Programada": True,
            "Estado": "PROGRAMADA",
            "Inicio": None,
            "Fin": None,
            "Horas": 0.0,
            "Personal_usado": None,
            "Equipos_usados": None,
            "Costo_personal": 0.0,
            "Costo_equipos": 0.0,
            "Costo_total": 0.0,
        }
        tareas_df = pd.concat([tareas_df, pd.DataFrame([new_row])], ignore_index=True)
        store["tareas"] = tareas_df
        save_store(store)
        st.success("Tarea programada y guardada ✅")

    st.divider()
    st.markdown("### 📋 Programadas por día")
    ver_dia = st.date_input("Ver programadas del día", value=date.today(), key="ver_prog_dia")
    df_dia = tareas_df.copy()
    df_dia["Fecha"] = pd.to_datetime(df_dia["Fecha"], errors="coerce")
    df_prog = df_dia[(df_dia["Fecha"].dt.date == ver_dia) & (df_dia["Estado"] == "PROGRAMADA")].copy()

    if df_prog.empty:
        st.info("No hay tareas programadas para ese día.")
    else:
        st.dataframe(df_prog[["ID","Fecha","Tipo_tarea","Prioridad","Nota","Estado"]].sort_values("ID"), use_container_width=True)

# TAB 2: Ejecutar (idéntico a tu lógica, guardando a Sheets)
with tabs[1]:
    st.subheader("⏱️ Ejecutar tareas (PROGRAMADA → EN_CURSO → FINALIZADA)")
    dia_trabajo = st.date_input("Día de trabajo", value=date.today(), key="dia_trabajo")

    df = tareas_df.copy()
    df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce")
    df_dia = df[df["Fecha"].dt.date == dia_trabajo].copy()

    colA, colB = st.columns(2)

    with colA:
        st.markdown("#### ✅ PROGRAMADAS (listas para iniciar)")
        prog = df_dia[df_dia["Estado"] == "PROGRAMADA"].copy().sort_values("ID")
        if prog.empty:
            st.info("No hay programadas para hoy.")
        else:
            st.dataframe(prog[["ID","Tipo_tarea","Prioridad","Nota"]], use_container_width=True, height=220)
            sel_id = st.selectbox("Selecciona ID para iniciar", prog["ID"].tolist(), key="sel_inicio_id")
            t_inicio = st.time_input("Hora inicio", value=datetime.now().time().replace(second=0, microsecond=0), key="t_inicio")

            if st.button("▶️ Iniciar tarea", key="btn_iniciar"):
                idx = tareas_df.index[tareas_df["ID"] == sel_id]
                if len(idx) == 1:
                    i = idx[0]
                    tareas_df.loc[i, "Inicio"] = combine_date_time(dia_trabajo, t_inicio)
                    tareas_df.loc[i, "Estado"] = "EN_CURSO"
                    store["tareas"] = tareas_df
                    save_store(store)
                    st.success("Iniciada y guardada ✅")
                    st.rerun()

    with colB:
        st.markdown("#### ⏳ EN CURSO (finalizar)")
        en_curso = df_dia[df_dia["Estado"] == "EN_CURSO"].copy().sort_values("ID")
        if en_curso.empty:
            st.info("No hay tareas en curso.")
        else:
            st.dataframe(en_curso[["ID","Tipo_tarea","Prioridad","Inicio"]], use_container_width=True, height=220)
            sel_fin_id = st.selectbox("Selecciona ID en curso", en_curso["ID"].tolist(), key="sel_fin_id")
            t_fin = st.time_input("Hora fin", value=datetime.now().time().replace(second=0, microsecond=0), key="t_fin")

            activos_personal = personal_df[(personal_df["Activo"] == True) & (personal_df["Nombre"].notna())].copy()
            activos_equipos = equipos_df[(equipos_df["Activo"] == True) & (equipos_df["Codigo"].notna())].copy()

            pers_sel = st.multiselect("Personal", options=activos_personal["Nombre"].tolist(), default=[], key="pers_sel")
            eq_sel = st.multiselect("Equipos", options=activos_equipos["Codigo"].tolist(), default=[], key="eq_sel")

            if st.button("🏁 Finalizar tarea", type="primary", key="btn_finalizar"):
                idx = tareas_df.index[tareas_df["ID"] == sel_fin_id]
                if len(idx) == 1:
                    i = idx[0]
                    inicio_val = tareas_df.loc[i, "Inicio"]
                    if pd.isna(inicio_val):
                        st.error("No tiene hora de inicio.")
                    else:
                        start_dt = pd.to_datetime(inicio_val).to_pydatetime()
                        end_dt = combine_date_time(dia_trabajo, t_fin)
                        horas = hours_between(start_dt, end_dt)

                        personal_cost = 0.0
                        if pers_sel:
                            cost_map = dict(zip(activos_personal["Nombre"], activos_personal["Costo_hora"]))
                            personal_cost = sum(safe_float(cost_map.get(n, 0.0), 0.0) for n in pers_sel) * horas

                        equipos_cost = 0.0
                        if eq_sel:
                            cost_map_e = dict(zip(activos_equipos["Codigo"], activos_equipos["Costo_hora"]))
                            equipos_cost = sum(safe_float(cost_map_e.get(c, 0.0), 0.0) for c in eq_sel) * horas

                        total_cost = personal_cost + equipos_cost

                        tareas_df.loc[i, "Fin"] = end_dt
                        tareas_df.loc[i, "Horas"] = float(round(horas, 2))
                        tareas_df.loc[i, "Personal_usado"] = ", ".join(pers_sel) if pers_sel else None
                        tareas_df.loc[i, "Equipos_usados"] = ", ".join(eq_sel) if eq_sel else None
                        tareas_df.loc[i, "Costo_personal"] = float(round(personal_cost, 2))
                        tareas_df.loc[i, "Costo_equipos"] = float(round(equipos_cost, 2))
                        tareas_df.loc[i, "Costo_total"] = float(round(total_cost, 2))
                        tareas_df.loc[i, "Estado"] = "FINALIZADA"

                        store["tareas"] = tareas_df
                        save_store(store)
                        st.success(f"Finalizada y guardada ✅ Total: {money(total_cost)}")
                        st.rerun()

# TAB 3: Personal (con botón guardar)
with tabs[2]:
    st.subheader("🧑‍🤝‍🧑 Personal (editable)")
    edited = st.data_editor(personal_df, use_container_width=True, num_rows="dynamic", hide_index=True, key="editor_personal")
    if st.button("💾 Guardar cambios de Personal"):
        personal_df = recalc_cost_hora_personal(edited)
        store["personal"] = personal_df
        save_store(store)
        st.success("Personal guardado ✅")
        st.rerun()

# TAB 4: Equipos (con botón guardar)
with tabs[3]:
    st.subheader("🚜 Equipos (editable)")
    edited_e = st.data_editor(equipos_df, use_container_width=True, num_rows="dynamic", hide_index=True, key="editor_equipos")
    if st.button("💾 Guardar cambios de Equipos"):
        equipos_df = recalc_cost_hora_equipos(edited_e)
        store["equipos"] = equipos_df
        save_store(store)
        st.success("Equipos guardados ✅")
        st.rerun()

# TAB 5: Dashboard (puedes pegar tu dashboard original aquí si quieres)
with tabs[4]:
    st.subheader("📊 Panel de control")
    st.info("Tu dashboard original lo puedes pegar aquí. La base ya guarda en Google Sheets.")

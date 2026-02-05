# app.py
# Costos por tarea + Tablero (S/.) — PRO+
# - Sin reportlab (NO PDF)
# - Exporta Excel (Detalle + Resúmenes)
# - Programar -> Iniciar -> Finalizar (costea al finalizar)
# - Dashboard profesional (Productividad del turno / Cumplimiento del plan)
# - Gráficos por prioridad, tipo, personal (tareas/horas/costo)

import os
import pickle
from datetime import datetime, date, time, timedelta

import pandas as pd
import streamlit as st
import altair as alt

from io import BytesIO
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter
from openpyxl.chart import BarChart, Reference
from openpyxl.chart.label import DataLabelList

# ---------------------------
# Helpers
# ---------------------------

DATA_FILE = "data_store.pkl"

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


def safe_float(x, default=0.0) -> float:
    try:
        if pd.isna(x):
            return default
        return float(x)
    except Exception:
        return default


def combine_date_time(d: date, t: time) -> datetime:
    return datetime(d.year, d.month, d.day, t.hour, t.minute, t.second)


def hours_between(start_dt: datetime, end_dt: datetime) -> float:
    """Hours diff >= 0. If end before start, assume crossed midnight (+1 day)."""
    if end_dt < start_dt:
        end_dt = end_dt + timedelta(days=1)
    return max((end_dt - start_dt).total_seconds() / 3600.0, 0.0)


def money(x: float) -> str:
    return f"S/ {x:,.2f}"


def is_saturday(d: date) -> bool:
    # Monday=0 ... Saturday=5 ... Sunday=6
    return d.weekday() == 5


def ensure_columns(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c not in df.columns:
            df[c] = None
    return df


def recalc_cost_hora_personal(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["Sueldo_mensual"] = df["Sueldo_mensual"].apply(lambda v: safe_float(v, 0.0))
    df["Horas_mes"] = df["Horas_mes"].apply(lambda v: safe_float(v, 0.0))
    df["Costo_hora"] = df.apply(
        lambda r: (safe_float(r["Sueldo_mensual"], 0.0) / safe_float(r["Horas_mes"], 0.0))
        if safe_float(r["Horas_mes"], 0.0) > 0
        else 0.0,
        axis=1,
    )
    return df


def recalc_cost_hora_equipos(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["Costo_mensual"] = df["Costo_mensual"].apply(lambda v: safe_float(v, 0.0))
    df["Horas_mes"] = df["Horas_mes"].apply(lambda v: safe_float(v, 0.0))
    df["Costo_hora"] = df.apply(
        lambda r: (safe_float(r["Costo_mensual"], 0.0) / safe_float(r["Horas_mes"], 0.0))
        if safe_float(r["Horas_mes"], 0.0) > 0
        else 0.0,
        axis=1,
    )
    return df


def default_data():
    # Personal EXACTO (según tu imagen)
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

    # Equipos (según tu imagen)
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
            "ID",
            "Fecha",
            "Tipo_tarea",
            "Prioridad",
            "Nota",
            "Programada",
            "Estado",  # PROGRAMADA / EN_CURSO / FINALIZADA
            "Inicio",
            "Fin",
            "Horas",
            "Personal_usado",
            "Equipos_usados",
            "Costo_personal",
            "Costo_equipos",
            "Costo_total",
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


def load_store():
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "rb") as f:
                store = pickle.load(f)
            # sanity
            if not isinstance(store, dict):
                return default_data()
            if "personal" not in store or "equipos" not in store or "tareas" not in store or "config" not in store:
                return default_data()
            return store
        except Exception:
            return default_data()
    return default_data()


def save_store(store):
    with open(DATA_FILE, "wb") as f:
        pickle.dump(store, f)


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
            text=alt.Text(y, format=",.2f" if df[y].dtype != "int64" else ".0f"),
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
            text=alt.Text(y, format=",.2f" if df[y].dtype != "int64" else ".0f"),
        )

    st.altair_chart((bars + text).interactive(), use_container_width=True)


def export_excel_report(
    dia: date,
    tareas_dia: pd.DataFrame,
    resumen: dict,
    por_personal: pd.DataFrame,
    por_tipo: pd.DataFrame,
) -> bytes:
    output = BytesIO()
    file_name = f"reporte_{dia.isoformat()}.xlsx"

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        # Hoja 1: Resumen
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

        # Hoja 2: Detalle del día
        detalle = tareas_dia.copy()
        for c in ["Inicio", "Fin"]:
            if c in detalle.columns:
                detalle[c] = detalle[c].astype(str)
        detalle.to_excel(writer, sheet_name="Detalle", index=False)

        # Hoja 3: Por personal
        por_personal.to_excel(writer, sheet_name="Por_personal", index=False)

        # Hoja 4: Por tipo de tarea
        por_tipo.to_excel(writer, sheet_name="Por_tipo", index=False)

    output.seek(0)

    # Ajustes + gráficos con openpyxl (simple y útil)
    wb = load_workbook(output)
    for ws_name in wb.sheetnames:
        ws = wb[ws_name]
        # autosize columns (simple)
        for col_idx, col in enumerate(ws.iter_cols(values_only=True), start=1):
            max_len = 0
            for v in col:
                if v is None:
                    continue
                max_len = max(max_len, len(str(v)))
            ws.column_dimensions[get_column_letter(col_idx)].width = min(max(max_len + 2, 12), 40)

    # Gráfico: Tareas por personal (cantidad)
    if "Por_personal" in wb.sheetnames:
        ws = wb["Por_personal"]
        # Esperamos columnas: Personal, Tareas, Horas, Costo_total
        headers = [c.value for c in ws[1]]
        if "Personal" in headers and "Tareas" in headers:
            col_personal = headers.index("Personal") + 1
            col_tareas = headers.index("Tareas") + 1
            if ws.max_row >= 2:
                chart = BarChart()
                chart.title = "Tareas por colaborador"
                chart.y_axis.title = "Tareas"
                data = Reference(ws, min_col=col_tareas, min_row=1, max_row=ws.max_row)
                cats = Reference(ws, min_col=col_personal, min_row=2, max_row=ws.max_row)
                chart.add_data(data, titles_from_data=True)
                chart.set_categories(cats)
                chart.dLbls = DataLabelList()
                chart.dLbls.showVal = True
                ws.add_chart(chart, "F2")

        if "Personal" in headers and "Horas" in headers:
            col_personal = headers.index("Personal") + 1
            col_horas = headers.index("Horas") + 1
            if ws.max_row >= 2:
                chart = BarChart()
                chart.title = "Horas por colaborador"
                chart.y_axis.title = "Horas"
                data = Reference(ws, min_col=col_horas, min_row=1, max_row=ws.max_row)
                cats = Reference(ws, min_col=col_personal, min_row=2, max_row=ws.max_row)
                chart.add_data(data, titles_from_data=True)
                chart.set_categories(cats)
                chart.dLbls = DataLabelList()
                chart.dLbls.showVal = True
                ws.add_chart(chart, "F18")

    # Guardar final a bytes
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
personal_df: pd.DataFrame = store["personal"]
equipos_df: pd.DataFrame = store["equipos"]
tareas_df: pd.DataFrame = store["tareas"]
config: dict = store["config"]

# Sidebar Configuración
st.sidebar.header("⚙️ Configuración")

config["horas_lv_efectivas"] = float(
    st.sidebar.number_input("Horas disponibles L-V (efectivas)", value=float(config.get("horas_lv_efectivas", 8.0)), step=0.25)
)
config["horas_sab_efectivas"] = float(
    st.sidebar.number_input("Horas disponibles Sábado (efectivas)", value=float(config.get("horas_sab_efectivas", 5.5)), step=0.25)
)

st.sidebar.caption("Horario informativo (solo referencia):")
config["inicio_lv"] = st.sidebar.text_input("Inicio L-V", value=str(config.get("inicio_lv", "07:30")))
config["fin_lv"] = st.sidebar.text_input("Fin L-V", value=str(config.get("fin_lv", "16:45")))
config["inicio_sab"] = st.sidebar.text_input("Inicio Sábado", value=str(config.get("inicio_sab", "07:30")))
config["fin_sab"] = st.sidebar.text_input("Fin Sábado", value=str(config.get("fin_sab", "13:00")))

st.sidebar.divider()
st.sidebar.caption("Tip: si editas el código, cierra con Ctrl+C y vuelve a ejecutar `python -m streamlit run app.py`")

# Normaliza columnas / recalcula
personal_df = ensure_columns(personal_df, ["Nombre", "Tipo", "Sueldo_mensual", "Horas_mes", "Activo", "Costo_hora"])
equipos_df = ensure_columns(equipos_df, ["Codigo", "Tipo", "Costo_mensual", "Horas_mes", "Activo", "Costo_hora"])
tareas_df = ensure_columns(
    tareas_df,
    [
        "ID",
        "Fecha",
        "Tipo_tarea",
        "Prioridad",
        "Nota",
        "Programada",
        "Estado",
        "Inicio",
        "Fin",
        "Horas",
        "Personal_usado",
        "Equipos_usados",
        "Costo_personal",
        "Costo_equipos",
        "Costo_total",
    ],
)

personal_df = recalc_cost_hora_personal(personal_df)
equipos_df = recalc_cost_hora_equipos(equipos_df)

# Persist immediately
store["personal"] = personal_df
store["equipos"] = equipos_df
store["tareas"] = tareas_df
store["config"] = config
save_store(store)

tabs = st.tabs(["🗓️ Programar", "⏱️ Ejecutar (Iniciar/Finalizar)", "🧑‍🤝‍🧑 Personal", "🚜 Equipos", "📊 Panel de control"])

# ---------------------------
# TAB 1: Programar
# ---------------------------
with tabs[0]:
    st.subheader("🗓️ Programar tareas (plan del día)")
    st.caption("Programa tareas (sin inicio/fin). Luego en Ejecutar las inicias y finalizas para calcular el costo.")

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
        st.success("Tarea programada.")

    st.divider()
    st.markdown("### 📋 Programadas por día")
    ver_dia = st.date_input("Ver programadas del día", value=date.today(), key="ver_prog_dia")
    df_dia = tareas_df.copy()
    df_dia["Fecha"] = pd.to_datetime(df_dia["Fecha"], errors="coerce")
    df_prog = df_dia[(df_dia["Fecha"].dt.date == ver_dia) & (df_dia["Estado"] == "PROGRAMADA")].copy()

    if df_prog.empty:
        st.info("No hay tareas programadas para ese día.")
    else:
        show_cols = ["ID", "Fecha", "Tipo_tarea", "Prioridad", "Nota", "Estado"]
        st.dataframe(df_prog[show_cols].sort_values("ID"), use_container_width=True)

# ---------------------------
# TAB 2: Ejecutar
# ---------------------------
with tabs[1]:
    st.subheader("⏱️ Ejecutar tareas (PROGRAMADA → EN_CURSO → FINALIZADA)")
    st.caption("Selecciona el día, inicia tareas programadas y luego finalízalas asignando personal/equipos para costear.")

    dia_trabajo = st.date_input("Día de trabajo", value=date.today(), key="dia_trabajo")

    df = tareas_df.copy()
    df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce")
    df_dia = df[df["Fecha"].dt.date == dia_trabajo].copy()

    colA, colB = st.columns(2)

    # PROGRAMADAS
    with colA:
        st.markdown("#### ✅ PROGRAMADAS (listas para iniciar)")
        prog = df_dia[df_dia["Estado"] == "PROGRAMADA"].copy().sort_values("ID")
        if prog.empty:
            st.info("No hay programadas para hoy.")
        else:
            st.dataframe(prog[["ID", "Tipo_tarea", "Prioridad", "Nota"]], use_container_width=True, height=220)

            ids_prog = prog["ID"].tolist()
            sel_id = st.selectbox("Selecciona ID para iniciar", ids_prog, key="sel_inicio_id")

            # hora inicio
            default_start = datetime.now().time().replace(second=0, microsecond=0)
            t_inicio = st.time_input("Hora inicio", value=default_start, key="t_inicio")

            if st.button("▶️ Iniciar tarea", key="btn_iniciar"):
                idx = tareas_df.index[tareas_df["ID"] == sel_id]
                if len(idx) == 1:
                    i = idx[0]
                    start_dt = combine_date_time(dia_trabajo, t_inicio)
                    tareas_df.loc[i, "Inicio"] = start_dt
                    tareas_df.loc[i, "Estado"] = "EN_CURSO"
                    tareas_df.loc[i, "Horas"] = 0.0
                    store["tareas"] = tareas_df
                    save_store(store)
                    st.success(f"Tarea {sel_id} iniciada.")
                    st.rerun()

    # EN CURSO / FINALIZAR
    with colB:
        st.markdown("#### ⏳ EN CURSO (finalizar)")
        en_curso = df_dia[df_dia["Estado"] == "EN_CURSO"].copy().sort_values("ID")

        if en_curso.empty:
            st.info("No hay tareas en curso.")
        else:
            st.dataframe(en_curso[["ID", "Tipo_tarea", "Prioridad", "Inicio", "Personal_usado", "Equipos_usados"]], use_container_width=True, height=220)

            ids_curso = en_curso["ID"].tolist()
            sel_fin_id = st.selectbox("Selecciona ID en curso", ids_curso, key="sel_fin_id")

            default_end = datetime.now().time().replace(second=0, microsecond=0)
            t_fin = st.time_input("Hora fin", value=default_end, key="t_fin")

            st.caption("Si faltó asignar operador/equipo, lo agregas aquí antes de finalizar.")

            activos_personal = personal_df[(personal_df["Activo"] == True) & (personal_df["Nombre"].notna())].copy()
            activos_equipos = equipos_df[(equipos_df["Activo"] == True) & (equipos_df["Codigo"].notna())].copy()

            pers_sel = st.multiselect(
                "Personal (editar)",
                options=activos_personal["Nombre"].tolist(),
                default=[],
                key="pers_sel",
            )
            eq_sel = st.multiselect(
                "Equipos (editar)",
                options=activos_equipos["Codigo"].tolist(),
                default=[],
                key="eq_sel",
            )

            if st.button("🏁 Finalizar tarea", type="primary", key="btn_finalizar"):
                idx = tareas_df.index[tareas_df["ID"] == sel_fin_id]
                if len(idx) == 1:
                    i = idx[0]
                    inicio_val = tareas_df.loc[i, "Inicio"]

                    if inicio_val is None or str(inicio_val) == "None" or pd.isna(inicio_val):
                        st.error("Esta tarea no tiene hora de inicio. Iníciala primero.")
                    else:
                        start_dt = pd.to_datetime(inicio_val).to_pydatetime()
                        end_dt = combine_date_time(dia_trabajo, t_fin)

                        horas = hours_between(start_dt, end_dt)

                        # Costos
                        # Personal cost: sum(costo_hora) * horas
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
                        st.success(f"Tarea {sel_fin_id} finalizada. Horas: {horas:.2f} | Total: {money(total_cost)}")
                        st.rerun()

    st.divider()
    st.markdown("### 🧾 Registro del día")
    df2 = tareas_df.copy()
    df2["Fecha"] = pd.to_datetime(df2["Fecha"], errors="coerce")
    df_dia_all = df2[df2["Fecha"].dt.date == dia_trabajo].copy().sort_values("ID")

    if df_dia_all.empty:
        st.info("No hay tareas registradas para este día.")
    else:
        show_cols = [
            "ID",
            "Fecha",
            "Tipo_tarea",
            "Prioridad",
            "Programada",
            "Inicio",
            "Fin",
            "Estado",
            "Horas",
            "Personal_usado",
            "Equipos_usados",
            "Costo_personal",
            "Costo_equipos",
            "Costo_total",
        ]
        st.dataframe(df_dia_all[show_cols], use_container_width=True)

# ---------------------------
# TAB 3: Personal
# ---------------------------
with tabs[2]:
    st.subheader("🧑‍🤝‍🧑 Personal (editable)")
    st.caption("Agrega/edita personal. Para renuncias: desmarca Activo. Costo/hora = Sueldo_mensual / Horas_mes.")

    st.markdown("#### ➕ Agregar personal rápido")
    c1, c2, c3 = st.columns([2, 1, 1])
    with c1:
        nuevo_nombre = st.text_input("Nombre", value="", key="nuevo_nombre")
    with c2:
        nuevo_tipo = st.selectbox("Tipo", TIPO_PERSONAL, key="nuevo_tipo")
    with c3:
        if st.button("Agregar", key="btn_add_personal"):
            if nuevo_nombre.strip():
                if (personal_df["Nombre"].astype(str).str.strip().str.lower() == nuevo_nombre.strip().lower()).any():
                    st.warning("Ese nombre ya existe. Si es otra persona, agrega un segundo nombre/apellido.")
                else:
                    add = pd.DataFrame([{
                        "Nombre": nuevo_nombre.strip(),
                        "Tipo": nuevo_tipo,
                        "Sueldo_mensual": 0.0,
                        "Horas_mes": 208,
                        "Activo": True,
                        "Costo_hora": 0.0
                    }])
                    personal_df = pd.concat([personal_df, add], ignore_index=True)
                    personal_df = recalc_cost_hora_personal(personal_df)
                    store["personal"] = personal_df
                    save_store(store)
                    st.success("Personal agregado.")
                    st.rerun()
            else:
                st.error("Escribe un nombre.")

    st.divider()

    edited = st.data_editor(
        personal_df,
        use_container_width=True,
        num_rows="dynamic",
        hide_index=True,
        column_config={
            "Nombre": st.column_config.TextColumn("Nombre"),
            "Tipo": st.column_config.SelectboxColumn("Tipo", options=TIPO_PERSONAL),
            "Sueldo_mensual": st.column_config.NumberColumn("Sueldo mensual (S/.)", step=50),
            "Horas_mes": st.column_config.NumberColumn("Horas mes", step=1),
            "Activo": st.column_config.CheckboxColumn("Activo"),
            "Costo_hora": st.column_config.NumberColumn("Costo_hora", disabled=True),
        },
        key="editor_personal",
    )

    personal_df = recalc_cost_hora_personal(edited)
    store["personal"] = personal_df
    save_store(store)

    st.markdown("#### Costo/hora calculado")
    st.dataframe(personal_df[["Nombre", "Tipo", "Activo", "Sueldo_mensual", "Horas_mes", "Costo_hora"]], use_container_width=True)

# ---------------------------
# TAB 4: Equipos
# ---------------------------
with tabs[3]:
    st.subheader("🚜 Equipos (editable)")
    st.caption("Agrega/edita equipos. Si un equipo sale de servicio: desmarca Activo. Costo/hora = Costo_mensual / Horas_mes.")

    edited_e = st.data_editor(
        equipos_df,
        use_container_width=True,
        num_rows="dynamic",
        hide_index=True,
        column_config={
            "Codigo": st.column_config.TextColumn("Código"),
            "Tipo": st.column_config.TextColumn("Tipo"),
            "Costo_mensual": st.column_config.NumberColumn("Costo mensual (S/.)", step=50),
            "Horas_mes": st.column_config.NumberColumn("Horas mes", step=1),
            "Activo": st.column_config.CheckboxColumn("Activo"),
            "Costo_hora": st.column_config.NumberColumn("Costo_hora", disabled=True),
        },
        key="editor_equipos",
    )

    equipos_df = recalc_cost_hora_equipos(edited_e)
    store["equipos"] = equipos_df
    save_store(store)

    st.markdown("#### Costo/hora calculado")
    st.dataframe(equipos_df[["Codigo", "Tipo", "Activo", "Costo_mensual", "Horas_mes", "Costo_hora"]], use_container_width=True)

# ---------------------------
# TAB 5: Dashboard
# ---------------------------
with tabs[4]:
    st.subheader("📊 Panel de control (gerencial)")
    st.caption("Resumen del día con métricas profesionales y gráficos por prioridad, tipo de tarea y colaborador.")

    dia = st.date_input("Día a analizar", value=date.today(), key="dia_dashboard")

    # Operativos en turno (para horas disponibles)
    operativos_turno = st.number_input("Operativos en turno (para horas disponibles)", min_value=0, value=6, step=1)

    incluir_admin_en_prod = st.checkbox("Incluir administrativos en horas productivas", value=False)

    df = tareas_df.copy()
    df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce")
    df_dia = df[df["Fecha"].dt.date == dia].copy()

    # métricas
    ops_total = int(len(df_dia))
    finalizadas = int((df_dia["Estado"] == "FINALIZADA").sum())
    pendientes = int(ops_total - finalizadas)

    horas_prod = float(pd.to_numeric(df_dia.loc[df_dia["Estado"] == "FINALIZADA", "Horas"], errors="coerce").fillna(0).sum())

    costo_total = float(pd.to_numeric(df_dia.loc[df_dia["Estado"] == "FINALIZADA", "Costo_total"], errors="coerce").fillna(0).sum())

    # horas disponibles por día
    horas_por_op = config["horas_sab_efectivas"] if is_saturday(dia) else config["horas_lv_efectivas"]
    horas_disp = float(operativos_turno) * float(horas_por_op)

    prod_pct = (horas_prod / horas_disp * 100.0) if horas_disp > 0 else 0.0

    programadas = int((df_dia["Programada"] == True).sum()) if "Programada" in df_dia.columns else ops_total
    cumpl_pct = (finalizadas / programadas * 100.0) if programadas > 0 else 0.0

    # KPIs en una grilla más ordenada
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Operaciones (día)", f"{ops_total}")
    k2.metric("Finalizadas", f"{finalizadas}")
    k3.metric("Pendientes", f"{pendientes}")
    k4.metric("Costo (finalizadas)", money(costo_total))
    k5.metric("Horas productivas", f"{horas_prod:.2f} h")

    k6, k7, k8 = st.columns(3)
    k6.metric("Horas disponibles", f"{horas_disp:.2f} h")
    k7.metric("Productividad del turno (%)", f"{prod_pct:.1f}%")
    k8.metric("Cumplimiento del plan (%)", f"{cumpl_pct:.1f}%")

    st.divider()

    # Gráficos (2 columnas)
    g1, g2 = st.columns(2)

    # Operaciones por prioridad (cantidad)
    with g1:
        st.markdown("### 🚦 Operaciones por prioridad (día)")
        tmp = df_dia.copy()
        tmp["Prioridad"] = tmp["Prioridad"].fillna("Sin prioridad")
        pr = tmp.groupby("Prioridad", as_index=False).size().rename(columns={"size": "Cantidad"})
        alt_bar_with_labels(pr, "Prioridad", "Cantidad", "Operaciones por prioridad", horizontal=False)

    # Horas por tipo de tarea
    with g2:
        st.markdown("### ⏱️ Horas por tipo de tarea (día)")
        tmp = df_dia[df_dia["Estado"] == "FINALIZADA"].copy()
        tmp["Horas"] = pd.to_numeric(tmp["Horas"], errors="coerce").fillna(0.0)
        tmp["Tipo_tarea"] = tmp["Tipo_tarea"].fillna("Sin tipo")
        ht = tmp.groupby("Tipo_tarea", as_index=False)["Horas"].sum()
        ht = ht.sort_values("Horas", ascending=False)
        alt_bar_with_labels(ht, "Tipo_tarea", "Horas", "Horas por tipo de tarea", horizontal=True)

    g3, g4 = st.columns(2)

    # Costo por tipo de tarea
    with g3:
        st.markdown("### 💰 Costo por tipo de tarea (día)")
        tmp = df_dia[df_dia["Estado"] == "FINALIZADA"].copy()
        tmp["Costo_total"] = pd.to_numeric(tmp["Costo_total"], errors="coerce").fillna(0.0)
        tmp["Tipo_tarea"] = tmp["Tipo_tarea"].fillna("Sin tipo")
        ct = tmp.groupby("Tipo_tarea", as_index=False)["Costo_total"].sum()
        ct = ct.sort_values("Costo_total", ascending=False)
        alt_bar_with_labels(ct, "Tipo_tarea", "Costo_total", "Costo por tipo de tarea (S/)", horizontal=True)

    # Resumen rápido
    with g4:
        st.markdown("### 🧾 Resumen rápido")
        st.write(
            f"""
- **Programadas (hoy):** {programadas}
- **En curso:** {int((df_dia["Estado"]=="EN_CURSO").sum())}
- **Finalizadas:** {finalizadas}
- **Costo del día:** {money(costo_total)}
"""
        )

    st.divider()

    # Por colaborador (tareas/horas/costo)
    st.markdown("## 👷‍♂️ Por colaborador (día)")

    fin = df_dia[df_dia["Estado"] == "FINALIZADA"].copy()

    # Expand Personal_usado (puede ser "A, B")
    records = []
    for _, r in fin.iterrows():
        personas = str(r.get("Personal_usado", "") or "").strip()
        if not personas:
            continue
        partes = [p.strip() for p in personas.split(",") if p.strip()]
        if not partes:
            continue
        horas = safe_float(r.get("Horas", 0.0), 0.0)
        costo = safe_float(r.get("Costo_total", 0.0), 0.0)
        for p in partes:
            # reparto simple: divide costo/horas entre participantes
            # (si quieres que NO se divida y se sume completo a cada uno, lo cambiamos)
            share = 1.0 / len(partes)
            records.append({"Personal": p, "Tareas": 1, "Horas": horas * share, "Costo_total": costo * share})

    por_personal = pd.DataFrame(records)
    if por_personal.empty:
        st.info("Aún no hay tareas finalizadas con personal asignado para graficar por colaborador.")
        por_personal_sum = pd.DataFrame(columns=["Personal", "Tareas", "Horas", "Costo_total"])
    else:
        por_personal_sum = (
            por_personal.groupby("Personal", as_index=False)
            .agg({"Tareas": "sum", "Horas": "sum", "Costo_total": "sum"})
            .sort_values(["Tareas", "Horas"], ascending=False)
        )

    cA, cB, cC = st.columns(3)
    with cA:
        alt_bar_with_labels(por_personal_sum, "Personal", "Tareas", "Tareas por colaborador", horizontal=True)
    with cB:
        alt_bar_with_labels(por_personal_sum, "Personal", "Horas", "Horas por colaborador", horizontal=True)
    with cC:
        alt_bar_with_labels(por_personal_sum, "Personal", "Costo_total", "Costo por colaborador (S/)", horizontal=True)

    st.divider()

    st.markdown("## 📄 Detalle del día")
    if df_dia.empty:
        st.info("No hay registros para este día.")
    else:
        show_cols = [
            "ID",
            "Fecha",
            "Tipo_tarea",
            "Prioridad",
            "Programada",
            "Inicio",
            "Fin",
            "Estado",
            "Horas",
            "Personal_usado",
            "Equipos_usados",
            "Costo_personal",
            "Costo_equipos",
            "Costo_total",
        ]
        st.dataframe(df_dia.sort_values("ID")[show_cols], use_container_width=True)

    # Export Excel (PRO)
    st.divider()
    st.markdown("## ⬇️ Reporte Excel (gerencial)")

    # Por tipo para export
    fin2 = df_dia[df_dia["Estado"] == "FINALIZADA"].copy()
    fin2["Horas"] = pd.to_numeric(fin2["Horas"], errors="coerce").fillna(0.0)
    fin2["Costo_total"] = pd.to_numeric(fin2["Costo_total"], errors="coerce").fillna(0.0)
    por_tipo = (
        fin2.groupby("Tipo_tarea", as_index=False)
        .agg(Tareas=("ID", "count"), Horas=("Horas", "sum"), Costo_total=("Costo_total", "sum"))
        .sort_values("Costo_total", ascending=False)
    )

    resumen = {
        "ops_total": ops_total,
        "finalizadas": finalizadas,
        "pendientes": pendientes,
        "horas_prod": float(round(horas_prod, 2)),
        "horas_disp": float(round(horas_disp, 2)),
        "prod_pct": float(round(prod_pct, 1)),
        "cumpl_pct": float(round(cumpl_pct, 1)),
        "costo_total": float(round(costo_total, 2)),
    }

    # hoja por_personal export con nombres esperados
    if por_personal_sum.empty:
        por_personal_export = pd.DataFrame(columns=["Personal", "Tareas", "Horas", "Costo_total"])
    else:
        por_personal_export = por_personal_sum.copy()
        por_personal_export["Horas"] = por_personal_export["Horas"].round(2)
        por_personal_export["Costo_total"] = por_personal_export["Costo_total"].round(2)

    xlsx_bytes = export_excel_report(
        dia=dia,
        tareas_dia=df_dia.sort_values("ID"),
        resumen=resumen,
        por_personal=por_personal_export,
        por_tipo=por_tipo,
    )

    st.download_button(
        "📥 Descargar Excel (reporte del día)",
        data=xlsx_bytes,
        file_name=f"reporte_{dia.isoformat()}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

# Final persist
store["personal"] = personal_df
store["equipos"] = equipos_df
store["tareas"] = tareas_df
store["config"] = config
save_store(store)

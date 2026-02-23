import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import pandas as pd
import re
from datetime import datetime




DATE_NAME_HINT = re.compile(r"(date|data|dt|dia|mes|month|ano|year)", re.IGNORECASE)

def _try_parse_datetime_series(s: pd.Series, sample_n: int = 200) -> float:
    if s.empty:
        return 0.0
    sample = s.dropna().astype(str).head(sample_n)
    if sample.empty:
        return 0.0
    parsed = pd.to_datetime(sample, errors="coerce", infer_datetime_format=True, dayfirst=True)
    return float(parsed.notna().mean())

def _colname_looks_like_date(colname: str) -> bool:
    
    try:
        pd.to_datetime(colname, errors="raise", dayfirst=True)
        return True
    except Exception:
        return False

def infer_format(df: pd.DataFrame) -> dict:

    notes = []
    if df is None or df.empty or df.shape[1] < 2:
        return {"format": "unknown", "date_col": None, "value_col": None, "date_cols": [], "key_cols": [], "notes": ["DataFrame vazio ou com poucas colunas."]}

    cols = list(df.columns)

    
    name_date_cols = [c for c in cols if _colname_looks_like_date(str(c))]

    
    parse_scores = {}
    for c in cols:
        if df[c].dtype == "O" or "date" in str(df[c].dtype).lower():
            parse_scores[c] = _try_parse_datetime_series(df[c])
        else:
            
            
            parse_scores[c] = _try_parse_datetime_series(df[c].astype(str))

    content_date_candidates = sorted(parse_scores.items(), key=lambda x: x[1], reverse=True)
    best_date_col, best_date_score = content_date_candidates[0]

    
    numeric_cols = [c for c in cols if pd.api.types.is_numeric_dtype(df[c])]
    
    if not numeric_cols:
        for c in cols:
            if c == best_date_col:
                continue
            coerced = pd.to_numeric(df[c], errors="coerce")
            if coerced.notna().mean() > 0.6:  
                numeric_cols.append(c)

    
    
    
    wide_score = 0
    if len(name_date_cols) >= 2 and len(name_date_cols) < len(cols):
        wide_score += 2
    if len(name_date_cols) >= 4:
        wide_score += 1

    
    
    
    long_score = 0
    if best_date_score >= 0.7:
        long_score += 2
    elif best_date_score >= 0.5:
        long_score += 1
    if len(numeric_cols) >= 1:
        long_score += 1

    
    if wide_score > long_score:
        fmt = "wide"
        date_cols = name_date_cols
        key_cols = [c for c in cols if c not in date_cols]
        notes.append(f"Inferido WIDE porque {len(date_cols)} colunas têm nome parseável como data.")
        return {"format": fmt, "date_col": None, "value_col": None, "date_cols": date_cols, "key_cols": key_cols, "notes": notes}

    if long_score > wide_score:
        fmt = "long"
        
        candidate_values = [c for c in numeric_cols if c != best_date_col]
        if candidate_values:
            value_col = max(candidate_values, key=lambda c: df[c].notna().mean())
        else:
            
            value_col = next((c for c in cols if c != best_date_col), None)

        key_cols = [c for c in cols if c not in {best_date_col, value_col}]
        notes.append(f"Inferido LONG porque '{best_date_col}' parece data (score={best_date_score:.2f}).")
        return {"format": fmt, "date_col": best_date_col, "value_col": value_col, "date_cols": [], "key_cols": key_cols, "notes": notes}

    fmt = "unknown"
    notes.append("Não foi possível inferir com confiança (empate de heurística). Use seleção manual.")
    return {"format": fmt, "date_col": best_date_col if best_date_score >= 0.5 else None,
            "value_col": numeric_cols[0] if numeric_cols else None,
            "date_cols": name_date_cols, "key_cols": [c for c in cols if c not in set(name_date_cols)], "notes": notes}





def ensure_dates_in_rows(df: pd.DataFrame, fmt_info: dict, date_col: str, value_col: str, key_cols: list[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    
    df2 = df.copy()

    
    if fmt_info["format"] == "wide":
        date_cols = fmt_info["date_cols"]
        key_cols = [c for c in df2.columns if c not in date_cols]
        melted = df2.melt(id_vars=key_cols, value_vars=date_cols, var_name="__date__", value_name="__value__")
        melted["__date__"] = pd.to_datetime(melted["__date__"], errors="coerce", dayfirst=True)
        
        melted["__value__"] = pd.to_numeric(melted["__value__"], errors="coerce")
        
        if key_cols:
            melted["__key__"] = melted[key_cols].astype(str).agg(" | ".join, axis=1)
        else:
            melted["__key__"] = "value"
        pivoted = melted.pivot_table(index="__date__", columns="__key__", values="__value__", aggfunc="sum")
        pivoted = pivoted.sort_index()
        pivoted.index.name = "date"
        return pivoted.reset_index()

    
    if not date_col or not value_col:
        raise ValueError("Para LONG, é necessário escolher date_col e value_col.")

    df2[date_col] = pd.to_datetime(df2[date_col], errors="coerce", dayfirst=True)
    df2[value_col] = pd.to_numeric(df2[value_col], errors="coerce")

    key_cols = [c for c in key_cols if c in df2.columns and c not in {date_col, value_col}]
    if key_cols:
        df2["__key__"] = df2[key_cols].astype(str).agg(" | ".join, axis=1)
    else:
        df2["__key__"] = "value"

    pivoted = df2.pivot_table(index=date_col, columns="__key__", values=value_col, aggfunc="sum")
    pivoted = pivoted.sort_index()
    pivoted.index.name = "date"
    return pivoted.reset_index()





class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Wide/Long Detector + Datas nas Linhas (Pivot)")
        self.geometry("1200x700")

        self.df = None
        self.fmt_info = None
        self.view_df = None

        
        top = ttk.Frame(self, padding=10)
        top.pack(side=tk.TOP, fill=tk.X)

        self.btn_load = ttk.Button(top, text="Carregar CSV/Excel", command=self.load_file)
        self.btn_load.pack(side=tk.LEFT)

        self.lbl_status = ttk.Label(top, text="Nenhum arquivo carregado.")
        self.lbl_status.pack(side=tk.LEFT, padx=10)

        
        mid = ttk.Frame(self, padding=10)
        mid.pack(side=tk.TOP, fill=tk.X)

        self.var_format = tk.StringVar(value="—")
        self.var_notes = tk.StringVar(value="")

        ttk.Label(mid, text="Formato detectado:").grid(row=0, column=0, sticky="w")
        ttk.Label(mid, textvariable=self.var_format, font=("Segoe UI", 10, "bold")).grid(row=0, column=1, sticky="w", padx=6)

        ttk.Label(mid, textvariable=self.var_notes, wraplength=900).grid(row=1, column=0, columnspan=6, sticky="w", pady=(4, 10))

        
        ttk.Label(mid, text="Date col:").grid(row=2, column=0, sticky="w")
        ttk.Label(mid, text="Value col:").grid(row=2, column=2, sticky="w", padx=(12, 0))
        ttk.Label(mid, text="Key cols (Ctrl+Click):").grid(row=2, column=4, sticky="w", padx=(12, 0))

        self.cmb_date = ttk.Combobox(mid, state="readonly", width=28, values=[])
        self.cmb_value = ttk.Combobox(mid, state="readonly", width=28, values=[])
        self.lst_keys = tk.Listbox(mid, selectmode=tk.MULTIPLE, height=6, exportselection=False)

        self.cmb_date.grid(row=2, column=1, sticky="w")
        self.cmb_value.grid(row=2, column=3, sticky="w")
        self.lst_keys.grid(row=2, column=5, sticky="w")

        btns = ttk.Frame(mid)
        btns.grid(row=3, column=0, columnspan=6, sticky="w", pady=(10, 0))

        self.btn_pivot = ttk.Button(btns, text="Gerar tabela (datas em linha)", command=self.generate_pivot, state="disabled")
        self.btn_pivot.pack(side=tk.LEFT)

        self.btn_export = ttk.Button(btns, text="Exportar pivot para CSV", command=self.export_csv, state="disabled")
        self.btn_export.pack(side=tk.LEFT, padx=8)

        
        bottom = ttk.Frame(self, padding=10)
        bottom.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

        self.tree = ttk.Treeview(bottom, show="headings")
        self.vsb = ttk.Scrollbar(bottom, orient="vertical", command=self.tree.yview)
        self.hsb = ttk.Scrollbar(bottom, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=self.vsb.set, xscrollcommand=self.hsb.set)

        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.vsb.pack(side=tk.RIGHT, fill=tk.Y)
        self.hsb.pack(side=tk.BOTTOM, fill=tk.X)

    def load_file(self):
        path = filedialog.askopenfilename(
            title="Selecione um CSV ou Excel",
            filetypes=[("CSV", "*.csv"), ("Excel", "*.xlsx;*.xls"), ("Todos", "*.*")]
        )
        if not path:
            return

        try:
            if path.lower().endswith(".csv"):
                
                try:
                    df = pd.read_csv(path, sep=None, engine="python")
                except Exception:
                    df = pd.read_csv(path)
            else:
                df = pd.read_excel(path)
        except Exception as e:
            messagebox.showerror("Erro ao carregar", str(e))
            return

        self.df = df
        self.fmt_info = infer_format(df)

        
        self.lbl_status.config(text=f"Carregado: {path.split('/')[-1]} | shape={df.shape}")
        self.var_format.set(self.fmt_info["format"].upper())
        self.var_notes.set(" | ".join(self.fmt_info["notes"]))

        cols = list(df.columns.astype(str))
        self.cmb_date["values"] = cols
        self.cmb_value["values"] = cols

        
        
        if self.fmt_info["format"] == "long" and self.fmt_info["date_col"] is not None:
            self.cmb_date.set(str(self.fmt_info["date_col"]))
        elif self.fmt_info.get("date_col"):
            self.cmb_date.set(str(self.fmt_info["date_col"]))
        else:
            self.cmb_date.set(cols[0] if cols else "")

        
        if self.fmt_info["format"] == "long" and self.fmt_info["value_col"] is not None:
            self.cmb_value.set(str(self.fmt_info["value_col"]))
        elif self.fmt_info.get("value_col"):
            self.cmb_value.set(str(self.fmt_info["value_col"]))
        else:
            self.cmb_value.set(cols[1] if len(cols) > 1 else "")

        
        self.lst_keys.delete(0, tk.END)
        for c in cols:
            self.lst_keys.insert(tk.END, c)

        
        suggested_keys = [str(c) for c in self.fmt_info.get("key_cols", [])]
        for i, c in enumerate(cols):
            if c in suggested_keys:
                self.lst_keys.selection_set(i)

        self.btn_pivot.config(state="normal")
        self.btn_export.config(state="disabled")

        
        self.show_df(df.head(200))

    def generate_pivot(self):
        if self.df is None:
            return

        try:
            
            if self.fmt_info["format"] == "wide":
                view_df = ensure_dates_in_rows(self.df, self.fmt_info, None, None, [])
            else:
                date_col = self.cmb_date.get().strip()
                value_col = self.cmb_value.get().strip()
                selected_key_idxs = list(self.lst_keys.curselection())
                key_cols = [self.lst_keys.get(i) for i in selected_key_idxs]
                view_df = ensure_dates_in_rows(self.df, self.fmt_info, date_col, value_col, key_cols)

            
            MAX_ROWS = 1000
            MAX_COLS = 60

            if view_df.shape[0] > MAX_ROWS:
                messagebox.showwarning("Aviso", f"Pivot tem {view_df.shape[0]} linhas. Mostrando apenas as primeiras {MAX_ROWS}.")
                view_df = view_df.head(MAX_ROWS)

            if view_df.shape[1] > MAX_COLS:
                messagebox.showwarning("Aviso", f"Pivot tem {view_df.shape[1]} colunas. Mostrando apenas as primeiras {MAX_COLS}.")
                view_df = view_df.iloc[:, :MAX_COLS]

            self.view_df = view_df
            self.show_df(view_df)
            self.btn_export.config(state="normal")

        except Exception as e:
            messagebox.showerror("Erro ao gerar pivot", str(e))

    def export_csv(self):
        if self.view_df is None or self.view_df.empty:
            return
        path = filedialog.asksaveasfilename(
            title="Salvar pivot como CSV",
            defaultextension=".csv",
            filetypes=[("CSV", "*.csv")]
        )
        if not path:
            return
        try:
            self.view_df.to_csv(path, index=False)
            messagebox.showinfo("OK", f"Exportado para:\n{path}")
        except Exception as e:
            messagebox.showerror("Erro ao exportar", str(e))

    def show_df(self, df: pd.DataFrame):
        
        self.tree.delete(*self.tree.get_children())

        cols = list(df.columns.astype(str))
        self.tree["columns"] = cols

        
        for c in cols:
            self.tree.heading(c, text=c)
            
            self.tree.column(c, width=140, anchor="w", stretch=True)

        
        for _, row in df.iterrows():
            values = []
            for c in cols:
                v = row[c]
                if pd.isna(v):
                    values.append("")
                elif isinstance(v, (pd.Timestamp, datetime)):
                    values.append(v.strftime("%Y-%m-%d"))
                else:
                    s = str(v)
                    if len(s) > 200:
                        s = s[:200] + "…"
                    values.append(s)
            self.tree.insert("", tk.END, values=values)


if __name__ == "__main__":
    app = App()
    app.mainloop()
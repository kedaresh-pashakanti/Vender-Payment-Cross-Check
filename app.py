# -*- coding: utf-8 -*-
import os
import streamlit as st
import pandas as pd
import polars as pl
import csv
import re
import zipfile
from io import BytesIO
from datetime import date

# =========================================================
# APP CONFIG
# =========================================================
st.set_page_config(page_title="SP Cross Check", layout="wide")
st.title("SP Cross Check")
st.markdown("Upload ZIP → Select MPR Date → Process → Summary → Download")

mpr_date = st.date_input("Select MPR Date", value=date.today())
MPR_DATE = mpr_date.strftime("%Y-%m-%d")

# "%Y-%m-%d"      # 2026-03-23
# "%d-%m-%Y"      # 23-03-2026
# "%d/%m/%Y"      # 23/03/2026
# "%m/%d/%Y"      # 03/23/2026
# "%d %b %Y"      # 23 Mar 2026
# "%d %B %Y"      # 23 March 2026

# =========================================================
# HELPERS
# =========================================================
def normalize_col_name(s):
    """Normalize a string for easy column comparison."""
    return re.sub(r"[^a-z0-9]", "", str(s).lower())

def normalize_path_name(s):
    """Normalize a file/folder path for matching vendor names inside ZIP."""
    return re.sub(r"[^a-z0-9]", "", str(s).lower())

def detect_delimiter_from_bytes(file_bytes):
    """Guess delimiter from file sample."""
    sample = file_bytes[:4096].decode("utf-8", errors="ignore")
    try:
        return csv.Sniffer().sniff(sample).delimiter
    except Exception:
        if "|" in sample:
            return "|"
        if "\t" in sample:
            return "\t"
        if "~" in sample:
            return "~"
        if "," in sample:
            return ","
        return ","

def to_numeric_series_cleanup(s):
    """Convert messy amount strings into numeric series."""
    if s is None:
        return pd.Series(dtype="float64")
    s = s.astype(str).str.replace(r"[^\d\.\-]", "", regex=True)
    s = pd.to_numeric(s.replace("", pd.NA), errors="coerce")
    return s

def calculate_sum(df, col):
    """Robust numeric sum."""
    if col not in df.columns:
        return 0.0
    return float(to_numeric_series_cleanup(df[col]).sum(skipna=True) or 0.0)

def add_mpr_column(df):
    """Insert MPR_Date column at index 0."""
    if "MPR_Date" in df.columns:
        df = df.drop(columns=["MPR_Date"], errors="ignore")
    df.insert(0, "MPR_Date", pd.to_datetime(MPR_DATE, errors="coerce").date())
    return df

def assign_numeric_columns(df, desired):
    """Assign given column names to first N columns."""
    cols_count = min(len(desired), df.shape[1])
    df = df.iloc[:, :cols_count].copy()
    df.columns = desired[:cols_count]
    return df

def find_header_row_and_reframe(df, keywords, max_scan_rows=15):
    """
    Find a row containing expected header keywords and use that row as the header.
    Useful for files where the real header is not the first row.
    """
    if df is None or df.empty:
        return df

    df_local = df.copy()
    nrows = min(len(df_local), max_scan_rows)

    for r in range(nrows):
        row_vals = df_local.iloc[r].astype(str).tolist()
        row_norm = [normalize_col_name(x) for x in row_vals]

        for k in keywords:
            key_norm = normalize_col_name(k)
            if any(key_norm == cell or key_norm in cell for cell in row_norm):
                header = [str(h).strip() for h in df_local.iloc[r].tolist()]
                new_df = df_local.iloc[r + 1 :].copy()
                new_df.columns = header
                new_df = new_df.loc[:, ~new_df.columns.duplicated()]
                new_df = new_df.reset_index(drop=True)
                return new_df

    return df_local

def find_best_column(df, candidates):
    """
    Find the best amount column from candidates.
    Falls back to amount/net/amt-like columns.
    """
    if df is None or df.empty:
        return None

    if not isinstance(candidates, (list, tuple)):
        candidates = [candidates]

    cols_map = {normalize_col_name(c): c for c in df.columns}

    # exact-ish matches
    for cand in candidates:
        if cand is None:
            continue
        key = normalize_col_name(cand)
        if key in cols_map:
            return cols_map[key]

    # soft matches for normalised candidate tokens
    candidate_tokens = [normalize_col_name(c) for c in candidates if c is not None]
    for norm, orig in cols_map.items():
        if any(tok and tok in norm for tok in candidate_tokens):
            return orig

    # fallback: common amount-ish headers
    for norm, orig in cols_map.items():
        if "amount" in norm or "amt" in norm or "net" in norm:
            return orig

    # last fallback: any header containing amount as raw text
    for orig in df.columns:
        if re.search(r"amount", str(orig), flags=re.I):
            return orig

    return None

def read_file_bytes_to_df(file_bytes, filename):
    """Generic reader for bytes + filename."""
    ext = filename.split(".")[-1].lower().strip()

    try:
        if ext in ["xlsx", "xls"]:
            df = pd.read_excel(BytesIO(file_bytes), engine="openpyxl")
        elif ext == "csv":
            try:
                df = pl.read_csv(BytesIO(file_bytes)).to_pandas()
            except Exception:
                delim = detect_delimiter_from_bytes(file_bytes)
                df = pd.read_csv(
                    BytesIO(file_bytes),
                    delimiter=delim,
                    engine="python",
                    dtype=str,
                    on_bad_lines="skip",
                )
        elif ext in ["txt", "rpt", "wri"]:
            delim = detect_delimiter_from_bytes(file_bytes)
            df = pd.read_csv(
                BytesIO(file_bytes),
                header=None,
                engine="python",
                dtype=str,
                on_bad_lines="skip",
                sep=delim,
            )
        else:
            df = pd.read_csv(
                BytesIO(file_bytes),
                engine="python",
                dtype=str,
                on_bad_lines="skip",
            )
    except Exception:
        try:
            df = pd.read_csv(BytesIO(file_bytes), dtype=str, encoding="utf-8")

        except UnicodeDecodeError:
            df = pd.read_csv(BytesIO(file_bytes), dtype=str, encoding="utf-16")

        except Exception:
            df = pd.read_csv(BytesIO(file_bytes), dtype=str, engine="python", errors="ignore")
            #df = pl.read_csv(BytesIO(file_bytes), infer_schema_length=0).to_pandas()
            df = df.astype(str)
        except Exception:
            df = pd.DataFrame()

    if df is None or not isinstance(df, pd.DataFrame):
        df = pd.DataFrame()

    df.columns = [str(c).strip() for c in df.columns]
    return df

def preprocess_for_vendor(df, key, file_bytes=None, filename=None):
    """
    Vendor-specific cleaning.
    Keep logic close to your current final.py behavior.
    """
    if df is None or not isinstance(df, pd.DataFrame):
        return pd.DataFrame()

    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    ext = (filename.split(".")[-1].lower().strip() if filename and "." in filename else "")

    # =========================
    # HDFC CARDS
    # =========================
    # if key == "hdfc_cards":
    #     # keep as-is; column detection will find Net_Amount / Net Amount
    #     pass
# =========================
# HDFC CARDS
# =========================
# =========================
# HDFC CARDS
# =========================
    if key == "hdfc_cards":
     try:
        df_calc = df.copy()

        recfmt_col = None
        for col in df_calc.columns:
            norm = normalize_col_name(col)
            if "recfmt" in norm or "rec_fmt" in norm:
                recfmt_col = col
                break

        if recfmt_col:
            df_calc[recfmt_col] = df_calc[recfmt_col].astype(str).str.strip().str.upper()
            df_calc = df_calc[df_calc[recfmt_col] == "BAT"].reset_index(drop=True)

        amount_col = None
        for col in df_calc.columns:
            norm = normalize_col_name(col)
            if "netamount" in norm or ("net" in norm and "amount" in norm):
                amount_col = col
                break

        if amount_col:
            df_calc[amount_col] = pd.to_numeric(
                df_calc[amount_col].astype(str).str.replace(",", ""),
                errors="coerce"
            )

        df.attrs["_calc_df"] = df_calc

     except Exception:
         pass
    # =========================
    # SBI ACQUIRING
    # =========================
    elif key == "sbi_acquiring":
        pass

    # =========================
    # SBI NB
    # =========================
    elif key == "sbi_nb":
        if df.shape[1] == 1:
            try:
                df = df.iloc[:, 0].astype(str).str.split("|", expand=True)
            except Exception:
                pass

        if all(str(c).isdigit() for c in df.columns):
            df = assign_numeric_columns(df, [
                "OnePay_Id", "Merchant_Id", "Amt", "Status", "Date_Of_Transaction"
            ])

    # =========================
    # ATOM NB
    # =========================
    elif key == "atom":
        try:
            # ATOM file has a title row first and actual header row on the second row.
            # Re-read raw bytes with header=None so we can use row 2 as header safely.
            if file_bytes is not None and ext in ["xlsx", "xls"]:
                raw_df = pd.read_excel(BytesIO(file_bytes), engine="openpyxl", header=None)
            else:
                raw_df = df.copy()

            if len(raw_df) >= 2:
                raw_df.columns = raw_df.iloc[1]
                raw_df = raw_df.iloc[2:].reset_index(drop=True)

            raw_df.columns = [str(c).strip().replace("\n", " ") for c in raw_df.columns]
            df = raw_df
        except Exception:
            pass

    # =========================
    # HDFC NB
    # =========================

    elif key == "hdfc_nb":
        try:
            file_obj = BytesIO(file_bytes) if file_bytes is not None else None

            if file_obj is not None:
                for sep in ["~", "|", ","]:
                    try:
                        file_obj.seek(0)
                        raw_df = pd.read_csv(file_obj, sep=sep, header=None, dtype=str, engine="python")

                        if raw_df.shape[1] > 5:
                            break

                    except Exception:
                        file_obj.seek(0)
                        continue

                # assign columns for final output
                df = assign_numeric_columns(raw_df, [
                    "Merchant_Code", "Code", "Currancy", "Amount", "NA",
                    "OnePay_Id", "Response_Code", "NA2", "Date_and_Time", "Response_Message"
                ])

                # keep a filtered copy only for total calculation
                df_calc = df.copy()

                if "Response_Code" in df_calc.columns:
                    df_calc["Response_Code"] = pd.to_numeric(df_calc["Response_Code"], errors="coerce")
                    df_calc = df_calc[df_calc["Response_Code"] == 0].reset_index(drop=True)
                if "Amount" in df_calc.columns:
                    df_calc["Amount"] = pd.to_numeric(
                    df_calc["Amount"].astype(str).str.replace(",", ""),
                    errors="coerce"
                    )
                df.attrs["_calc_df"] = df_calc
        except Exception:
            pass
 
 
    # =========================
    # AXIS NB
    # =========================
    elif key == "axis_nb":
        try:
        # 🔥 FIX: split if single column
         file_obj = BytesIO(file_bytes)

        # 🔥 TRY MULTIPLE DELIMITERS AT READ TIME
         for sep in ["|", "~", ",", "\t"]:
            try:
                file_obj.seek(0)
                df = pd.read_csv(file_obj, sep=sep, header=None, dtype=str, engine="python")

                # if proper columns mil gaye → break
                if df.shape[1] >= 6:
                    break
            except:
                file_obj.seek(0)
                continue

        # 🔥 assign columns
         df = assign_numeric_columns(df, [
            "BID", "User_ID", "User_Name", "ITC_No",
            "PRN_No", "Amount", "Transaction_Date", "Status"
        ])

        # remove duplicate header row
         if not df.empty:
            df = df[df.iloc[:, 0] != df.columns[0]].reset_index(drop=True)

        # remove last row
         if len(df) > 0:
            df = df.iloc[:-1].reset_index(drop=True)
        except Exception:
            pass

    # =========================
    # YES NB
    # =========================
    elif key == "yes_nb":
        try:

         df = find_header_row_and_reframe(
            df,
            ["Amount", "Service Charges", "Bank Reference"]
        )

        # 🔥 Normalize columns
         col_map = {}
         for col in df.columns:
            norm = normalize_col_name(col)

            if "merchantcode" in norm:
                col_map[col] = "Merchant_Code"
            elif "clientcode" in norm:
                col_map[col] = "Client_Code"
            elif "merchantreference" in norm:
                col_map[col] = "Merchant_Reference"
            elif "transactiondate" in norm:
                col_map[col] = "Transaction_Date"
            elif norm == "amount":
                col_map[col] = "Amount"
            elif "servicecharges" in norm:
                col_map[col] = "Service_Charges"
            elif "bankreference" in norm:
                col_map[col] = "Bank_Reference"
            elif "transactionstatus" in norm:
                col_map[col] = "Transaction_Status"

         df = df.rename(columns=col_map)

        # 🔥 Ensure required columns exist
         required_cols = [
            "Merchant_Code","Client_Code","Merchant_Reference",
            "Transaction_Date","Amount","Service_Charges",
            "Bank_Reference","Transaction_Status"
        ]

         for col in required_cols:
            if col not in df.columns:
                df[col] = ""

        # 🔥 FIX 1: Treat as TEXT (no scientific / no .0)
         if "Merchant_Reference" in df.columns:
            df["Merchant_Reference"] = df["Merchant_Reference"].astype(str).str.replace(r"\.0$", "", regex=True)

         if "Bank_Reference" in df.columns:
            df["Bank_Reference"] = df["Bank_Reference"].astype(str).str.replace(r"\.0$", "", regex=True)

        # 🔥 FIX 2: Format Transaction Date
         if "Transaction_Date" in df.columns:
            df["Transaction_Date"] = pd.to_datetime(
                df["Transaction_Date"], errors="coerce"
            ).dt.strftime("%Y-%m-%d %H:%M:%S")

        # 🔥 Add MPR_Date
         df["MPR_Date"] = MPR_DATE

        # 🔥 Final column order
         df = df[[
            "MPR_Date","Merchant_Code","Client_Code","Merchant_Reference",
            "Transaction_Date","Amount","Service_Charges",
            "Bank_Reference","Transaction_Status"
        ]]
        except Exception:
            pass

    # =========================
    # ICICI NB
    # =========================
    elif key == "icici_nb":
        if all(str(c).isdigit() for c in df.columns) or len(df.columns) < 5:
            df = find_header_row_and_reframe(
                df,
                ["OnePay_Id", "RRN", "Code", "Amount", "Transaction_Date"]
            )

            if all(str(c).isdigit() for c in df.columns):
                df = assign_numeric_columns(df, [
                    "OnePay_Id", "RRN", "Code", "Amount", "Transaction_Date"
                ])

    # =========================
    # HDFC UPI
    # =========================
    elif key == "hdfc_upi":
        try:
            # Re-read HDFC UPI as text so 19-digit Order IDs stay exact.
            # This is limited to HDFC UPI only and does not affect other vendors.
            if file_bytes is not None and filename and filename.split(".")[-1].lower().strip() in ["xlsx", "xls"]:
                df = pd.read_excel(BytesIO(file_bytes), engine="openpyxl", dtype=str)
            else:
                df = df.astype(str)

            df.columns = [str(c).strip() for c in df.columns]
            df_calc = df.copy(deep=True)

            # Fix Order ID (remove first 3 alphabets like PAQ)
            for col in df.columns:
                if "order" in normalize_col_name(col) and "id" in normalize_col_name(col):
                    df[col] = df[col].astype(str).apply(
                        lambda x: x[3:] if isinstance(x, str) and re.match(r"^[A-Za-z]{3}\d+", x) else x
                    )

            # Fix Txn ref no. (RRN) → force as text
            for col in df.columns:
                norm = normalize_col_name(col)
                if "txnref" in norm or "rrn" in norm:
                    df[col] = df[col].apply(
                        lambda x: str(x).replace(".0", "") if pd.notna(x) else x
                    )

            # keep a filtered copy only for total calculation
            if "CR/DR" in df_calc.columns:
                df_calc = df_calc[df_calc["CR/DR"] == "CR"].reset_index(drop=True)

            df.attrs["_calc_df"] = df_calc
        except Exception:
            pass





    # =========================
    # WORLDLINE NB
    # =========================
    elif key == "worldline_nb":
        try:
        # 🔥 Treat IDs as TEXT
            if "SM Transaction Id" in df.columns:
             df["SM Transaction Id"] = df["SM Transaction Id"].astype(str).str.replace(r"\.0$", "", regex=True)

            if "Bank_Transaction_id" in df.columns:
             df["Bank_Transaction_id"] = df["Bank_Transaction_id"].astype(str).str.replace(r"\.0$", "", regex=True)
        except Exception:
            pass
    # =========================
    # ICICI CARDS
    # =========================
    elif key == "icici_cards":
        pass

    # =========================
    # BILDESK
    # =========================
    elif key == "bildesk":
        numeric_cols = [str(c).isdigit() or isinstance(c, int) for c in df.columns]

        if all(numeric_cols):
            df = assign_numeric_columns(df, [
                "Biller_Id", "Bank_Id", "Bank_Ref_No", "PGI_Ref_No", "Ref_1",
                "Ref_2", "Ref_3", "Ref_4", "Ref_5", "Ref_6", "Ref_7", "Ref_8",
                "Filler", "Date_of_Txn", "Settlement_Date", "Gross_Amount",
                "Charges", "GST", "Net_Amount"
            ])
        else:
            df = find_header_row_and_reframe(
                df,
                ["Biller_Id", "Date_of_Txn", "Net_Amount", "Gross_Amount"]
            )
        # FIX: Remove 2nd row only for Bildesk
        if len(df) > 1:
            df = df.drop(df.index[0]).reset_index(drop=True)

    # =========================
    # 1PAYECMSHDFC
    # =========================
    elif key == "1payecmshdfc":
        pass

    # =========================
    # KOTAK UPI
    # =========================
    elif key == "kotak_upi":
        try:
            # Re-read ONLY for Kotak as string (no impact on others)
            if file_bytes is not None and ext in ["xlsx", "xls"]:
                df = pd.read_excel(BytesIO(file_bytes), engine="openpyxl", dtype=str)

            for col in df.columns:
                norm = normalize_col_name(col)

                if (
                    "refid" in norm or
                    "payeeaccountnumber" in norm or
                    "nvltsdkorderidcbsorderid" in norm
                ):
                    df[col] = df[col].astype(str).str.replace(r"\.0$", "", regex=True)

        except Exception:
            pass

    # final cleanup
    df.columns = [str(c).strip() for c in df.columns]
    return df

# =========================================================
# ZIP HELPERS
# =========================================================
def find_vendor_files(zip_names, vendor_name, vendor_key):
    """
    Return all non-directory members inside ZIP whose path includes
    the vendor name / key.
    """
    matches = []

    aliases = [
        vendor_name,
        vendor_key,
        vendor_name.replace(" ", ""),
        vendor_key.replace("_", ""),
    ]
    aliases = [normalize_path_name(t) for t in aliases if t]

    for name in zip_names:
        if name.endswith("/"):
            continue

        normalized_full = normalize_path_name(name)
        path_parts = [normalize_path_name(part) for part in name.split("/")]

        if any(
            tok and (
                tok in normalized_full or any(tok in part for part in path_parts)
            )
            for tok in aliases
        ):
            matches.append(name)

    # remove duplicates while preserving order
    seen = set()
    unique = []
    for x in matches:
        if x not in seen:
            unique.append(x)
            seen.add(x)
    return unique

def process_vendor_files(zip_ref, zip_names, vendor):
    """
    Process one vendor from the ZIP.
    Returns: (status_text, total, merged_df, error_text)
    """
    key = vendor["key"]
    name = vendor["name"]
    candidates = vendor["candidates"]
    mode = vendor["mode"]

    files = find_vendor_files(zip_names, name, key)

    if not files:
        return f"❌ {name}: file not found in ZIP", None, None, "file not found in ZIP"

    # For single mode, take first file only. For multiple, use all.
    if mode == "single":
        files = files[:1]

    processed_frames = []
    calc_frames = []

    for fpath in files:
        try:
            file_bytes = zip_ref.read(fpath)
            st.write(f"📂 Reading {fpath}")
            df = read_file_bytes_to_df(file_bytes, fpath)
            df = preprocess_for_vendor(df, key, file_bytes=file_bytes, filename=fpath)

            if isinstance(df, pd.DataFrame):
             if "_calc_df" in df.attrs and isinstance(df.attrs["_calc_df"], pd.DataFrame):
              calc_df = df.attrs["_calc_df"].copy()
             else:
              calc_df = df.copy()

             calc_frames.append(calc_df)
             # 🔥 FIX: ALWAYS use ORIGINAL df for output
             processed_frames.append(df.copy(deep=True))
               
            # if df is None or df.empty:
            #     processed_frames.append(df)
            # else:
            #     processed_frames.append(df)
        except Exception as e:
            return f"❌ {name}: {e}", None, None, str(e)

    # merge if needed
    if len(processed_frames) == 0:
        return f"❌ {name}: no readable files", None, None, "no readable files"

    if len(processed_frames) == 1:
        merged = processed_frames[0].copy()
    else:
        merged = pd.concat(processed_frames, ignore_index=True, sort=False)

    if len(calc_frames) == 0:
        calc_merged = merged.copy()
        calc_merged = calc_merged.copy()
    elif len(calc_frames) == 1:
        calc_merged = calc_frames[0].copy()
    else:
        calc_merged = pd.concat(calc_frames, ignore_index=True, sort=False)

    # figure out amount column
    amount_col = find_best_column(calc_merged, candidates)
    if not amount_col:
        return (
            f"❌ {name}: Column not found. Available: {list(merged.columns)}",
            None,
            merged,
            "column not found",
        )

    total = calculate_sum(calc_merged, amount_col)
    merged = add_mpr_column(merged)

    return f"✅ {name}: Done", total, merged, None

# =========================================================
# VENDOR CONFIG
# =========================================================
vendors = [
    {"key": "hdfc_cards",   "name": "HDFC Cards",     "candidates": ["Net_Amount"],               "mode": "multiple"},
    {"key": "sbi_acquiring","name": "SBI Acquiring",  "candidates": ["NET_AMT"],                  "mode": "multiple"},
    {"key": "sbi_nb",       "name": "SBI NB",         "candidates": ["Amt"],                      "mode": "multiple"},
    {"key": "atom",         "name": "ATOM NB",        "candidates": ["Net Amount to be Paid"],    "mode": "multiple"},
    {"key": "hdfc_nb",      "name": "HDFC NB",        "candidates": ["Amount"],                   "mode": "multiple"},
    {"key": "axis_nb",      "name": "AXIS NB",        "candidates": ["Amount"],                   "mode": "multiple"},
    {"key": "yes_nb",       "name": "YES NB",         "candidates": ["Amount", "Amount_guess"],   "mode": "multiple"},
    {"key": "icici_nb",     "name": "ICICI NB",       "candidates": ["Amount"],                   "mode": "multiple"},
    {"key": "hdfc_upi",     "name": "HDFC UPI",       "candidates": ["Net_Amount", "Net Amount"], "mode": "multiple"},
    {"key": "worldline_nb", "name": "Worldline NB",   "candidates": ["Net_Amount", "Net Amount", "Amount", "Amount_guess"], "mode": "multiple"},
    {"key": "icici_cards",  "name": "ICICI Cards",    "candidates": ["Net_Amount"],               "mode": "multiple"},
    {"key": "bildesk",      "name": "Bildesk",        "candidates": ["Net_Amount"],               "mode": "multiple"},
    {"key": "1payecmshdfc", "name": "1PayecmsHDFC",   "candidates": ["Amount"],                   "mode": "multiple"},
    {"key": "kotak_upi",    "name": "Kotak UPI",      "candidates": ["NET_AMOUNT", "Amount"],     "mode": "multiple"},
]

# =========================================================
# OUTPUT SHEET SCHEMAS
# =========================================================
INDIANBANK_UPI_COLUMNS = [
    "MPR_Date", "SR_NO", "MERCHANT_ACC_NO", "AMOUNT", "DATETIMEOFTRANSACTION",
    "FROM_VPA", "RRN/UTRNO", "TRANSACTION_ID", "REF_ID", "TRANSACTION_STATUS",
    "ERROR_DESCRIPTION", "MERCHANT_VPA", "REMARKS"
]

YES_BANK_COLUMNS = [
    "MPR_Date", "Merchant_Code", "Client_Code", "Merchant_Reference",
    "Transaction_Date", "Amount", "Service_Charges", "Bank_Reference",
    "Transaction_Status"
]

PAYOUT_MASTER_COLUMNS = [
    "PayoutDate", "TRANSACTION_DATE", "MERCHANT_TRANSACTION_ID", "TRANSACTION_ID",
    "MERCHANT_ID", "MERCHANT_NAME", "RESELLER_ID", "RESELLER_NAME", "PRODUCT_ID",
    "SP_ID", "SP_NAME", "INSTRUMENT_ID", "BANK_ID", "MOBILE_NO", "EMAIL_ID",
    "UDF1", "UDF2", "UDF3", "UDF4", "UDF5", "UDF6", "RECON_STATUS", "NARRATION",
    "RECON_DATE", "COMMISSION_PERCENTAGE", "COMMISSION_FLAT", "TRANSACTION_AMOUNT",
    "POSTING_AMOUNT", "MDR", "GST_ON_MDR", "SURCHARGE", "NET_SETTLEMENT_AMOUNT",
    "MERCHANT_SETTLEMENT_AMOUNT", "REFUND_AMOUNT", "BANK_CHARGES",
    "GST_ON_BANK_CHARGES", "RESELLER_CHARGES", "GST_ON_RESELLER_CHARGES",
    "TOTAL_CHARGES", "NET_PROFIT", "REFUND_TYPE", "BANK_REF_NO", "IS_GENERATED",
    "PAYOUT_REF_NO", "UTR_NO", "INCOMMING_UTR", "OUTGOING_UTR", "UTR_UDF",
    "GENERATED_BY", "MODIFIED_BY", "ArnNo", "RefundRequestId", "ReconId",
    "payout_escrow", "CB_AMOUNT", "CB_Status", "MPR_STATUS", "MPR_AMOUNT",
    "MPR_DATE", "MPR_RRN", "MPR_UDF1", "MPR_UDF2", "MPR_UDF3", "MPR_UDF4",
    "MPR_UDF5", "is_tsp", "SETTLEMENT_CYCLE", "PAYOUT_BY", "is_gross", "cycle",
    "GENERATED_ON", "MODIFIED_ON", "SourceFileName"
]

OUTPUT_SHEET_SPECS = [
    ("1indianbankupi", None, INDIANBANK_UPI_COLUMNS),
    ("2hdfc", "HDFC Cards", None),
    ("3sbi_acquiring", "SBI Acquiring", None),
    ("4sbi_nb", "SBI NB", None),
    ("5atom_nb", "ATOM NB", None),
    ("6hdfc_nb", "HDFC NB", None),
    ("7axis_bank_nb", "AXIS NB", None),
    ("8yes_bank_nb", "YES NB", YES_BANK_COLUMNS),
    ("9icici_nb", "ICICI NB", None),
    ("10hdfcupi", "HDFC UPI", None),
    ("12worldline_nb", "Worldline NB", None),
    ("13icicicards", "ICICI Cards", None),
    ("151payecmshdfc", "1PayecmsHDFC", None),
    ("18billdesk", "Bildesk", None),
    ("20kotakupi", "Kotak UPI", None),
    ("payout_master", None, PAYOUT_MASTER_COLUMNS),
]


EXACT_OUTPUT_SCHEMAS = {
    "1indianbankupi": [
        "MPR_Date", "SR_NO", "MERCHANT_ACC_NO", "AMOUNT", "DATETIMEOFTRANSACTION",
        "FROM_VPA", "RRN/UTRNO", "TRANSACTION_ID", "REF_ID", "TRANSACTION_STATUS",
        "ERROR_DESCRIPTION", "MERCHANT_VPA", "REMARKS"
    ],
    "2hdfc": [
        "MPR_Date", "MERCHANT_CODE", "TERMINAL_NUMBER", "REC_FMT", "BAT_NBR",
        "CARD_TYPE", "CARD_NUMBER", "TRANS_DATE", "SETTLE_DATE", "APPROV_CODE",
        "INTNL_AMT", "DOMESTIC_AMT", "TRAN_ID", "UPVALUE", "MERCHANT_TRACKID",
        "MSF", "SERV_TAX", "SB_Cess", "KK_Cess", "CGST_AMT", "SGST_AMT",
        "IGST_AMT", "UTGST_AMT", "Net_Amount", "DEBITCREDIT_TYPE", "UDF1",
        "UDF2", "UDF3", "UDF4", "UDF5", "SEQUENCE_NUMBER", "ARN_NO",
        "INVOICE_NUMBER", "GSTN_TRANSACTION_ID"
    ],
    "3sbi_acquiring": [
        "MPR_Date", "SUPERMERCHANT_ID", "MERCHANT_ID", "TERMINAL_ID", "CARD_NO",
        "TRANSACTION_TYPE", "TRAN_DATE", "TRACE_NO", "TXN_REF", "MERCHANT_TXNNO",
        "EPG_TXN_REFNO", "APPROVE_CODE", "GROSS_AMT", "MDR", "NET_AMT",
        "TXN_AMT", "TXN_CURR", "ARN", "INST_BASE_CURR", "MAP_PAYM_NAME",
        "MTS_TRAN_MCC", "MGI_ITCH_NAME", "ONUS_INDC", "MTS_MSF_PERC",
        "MTS_MSF_FIXFEE", "MTS_CSF_PERC", "MTS_CSF_FIXFEE", "MTS_TOTL_CSF_AMT",
        "VAT_AMT", "PRCHS_RRN", "PRCHS_PG_TXN_REFNO", "PAYMENT_ID",
        "PRCHS_MERCHANT_TXNNO"
    ],
    "4sbi_nb": [
        "MPR_Date", "OnePay_Id", "Merchant_Id", "Amt", "Status", "Date_Of_Transaction"
    ],
    "5atom_nb": [
        "MPR_Date", "Merchant_Name", "Merchant_ID", "Atom_Txn_ID", "Txn_State",
        "Txn_Date", "Client_Code", "Merchant_Txn_ID", "Product", "Discriminator",
        "Bank_Card_Name", "Card_Type", "Card_No", "Card_Issuing_Bank",
        "Bank_Ref_No", "Refund_Ref_No", "Gross_Txn_Amount", "Txn_Charges",
        "Service_Tax", "SB_Cess", "Krishi_Kalyan_Cess", "Total_Chargeable",
        "Net_Amount_to_be_Paid", "Payment_Status", "Settlement_Date",
        "Refund_Status", "UTR_Number", "udf1", "udf2", "udf3", "udf4", "udf5",
        "udf6", "udf7", "udf9", "udf10", "udf11", "udf12", "udf13", "udf14",
        "udf15", "udfex1", "udfex2", "udfex3", "udfex4", "udfex5", "udfex6",
        "udfex7", "udfex8", "udfex9", "udfex10", "UMN", "executionTxnId"
    ],
    "6hdfc_nb": [
        "MPR_Date", "Merchant_Code", "Code", "Currancy", "Amount", "NA",
        "OnePay_Id", "Response_Code", "NA2", "Date_and_Time", "Response_Message"
    ],
    "7axis_bank_nb": [
        "MPR_Date", "BID", "User_ID", "User_Name", "ITC_No", "PRN_No",
        "Amount", "Transaction_Date", "Status"
    ],
    "8yes_bank_nb": [
        "MPR_Date", "Merchant_Code", "Client_Code", "Merchant_Reference",
        "Transaction_Date", "Amount", "Service_Charges", "Bank_Reference",
        "Transaction_Status"
    ],
    "9icici_nb": [
        "MPR_Date", "OnePay_Id", "RRN", "Code", "Amount", "Transaction_Date"
    ],
    "10hdfcupi": [
        "MPR_Date", "External_MID", "External_TID", "UPI_Merchant_ID",
        "Merchant_Name", "Merchant_VPA", "Payer_VPA", "UPI_Trxn_ID", "Order_ID",
        "Txn_ref_no_RRN", "Transaction_Req_Date", "Settlement_Date", "Currency",
        "Transaction_Amount", "MSF_Amount", "CGST_AMT", "SGST_AMT", "IGST_AMT",
        "UTGST_AMT", "Net_Amount", "GST_Invoice_No", "Trans_Type", "Pay_Type",
        "CR_DR", "Additional_Field_1", "Additional_Field_2", "Additional_Field_3",
        "Additional_Field_4", "Additional_Field_5", "Future_Free_Field_1",
        "Future_Free_Field_2", "Future_Free_Field_3", "Future_Free_Field_4",
        "Future_Free_Field_5", "Future_Free_Field_6", "Future_Free_Field_7",
        "Future_Free_Field_8", "Future_Free_Field_9", "Future_Free_Field_10"
    ],
    "12worldline_nb": [
        "MPR_Date", "SR_No", "Bank_Id", "Bank_Name", "TPSL_Transaction_id",
        "SM_Transaction_Id", "Bank_Transaction_id", "Total_Amount", "Charges",
        "Taxes", "Net_Amount", "Transaction_Date", "Transaction_Time",
        "Payment_Date", "SRC_ITC", "Merchant_ID", "Payment_Mode"
    ],
    "13icicicards": [
        "MPR_Date", "Legal_name", "Card_Acceptor_Identification_Code",
        "Card_Acceptor_Terminal_Identification", "Unique_ID", "Txn_Type",
        "Date_Local_Transaction", "Time_Local_Transaction", "Retrieval_Ref_number",
        "Network_Interface", "PAN_masked", "Issuing_Bank_name", "Amount_Transaction",
        "Amount_Settlement", "Settlement_status", "Date_Settlement",
        "Void_Reversal_Indicator", "Transaction_Source", "Network_interchange_fees",
        "Network_other_fees", "Type_of_Transaction", "ARN", "MDR", "MDR_GST",
        "Gross_Amount", "Net_Amount", "Order_id"
    ],
    "151payecmshdfc": [
        "MPR_Date", "Posting_date", "Account_No", "Client_code", "Client_name",
        "Reference_No", "Amount", "Remitter_Account", "Remitter_Name",
        "Remitter_Bank", "Remitter_IFSC"
    ],
    "18billdesk": [
        "MPR_Date", "Biller_Id", "Bank_Id", "Bank_Ref_No", "PGI_Ref_No",
        "Ref_1", "Ref_2", "Ref_3", "Ref_4", "Ref_5", "Ref_6", "Ref_7", "Ref_8",
        "Filler", "Date_of_Txn", "Settlement_Date", "Gross_Amount", "Charges",
        "GST", "Net_Amount"
    ],
    "20kotakupi": [
        "MPR_Date", "AGGREGATORCODE", "MERCHANTID", "REFID", "TRANSACTION_DATE",
        "PAYEE_VPA", "Payee_ACCOUNT_NUMBER", "Payee_IFSC_Code",
        "PAYER_ACCOUNT_NUMBER", "PAYER_IFSC", "PAYER_VPA", "PAYER_NAME",
        "AMOUNT", "RESPONSE_CODE", "TRANSACTION_ID", "DEBIT_NBIN",
        "TRANSACTION_STATUS", "MCC_CODE", "MDR_CHARGED", "GST", "NET_AMOUNT",
        "NVL_TSDK_ORDERID_CBS_ORDERID", "NVL_TSDK_REMARKS_CBS_REMARKS"
    ],
    "payout_master": [
        "PayoutDate", "TRANSACTION_DATE", "MERCHANT_TRANSACTION_ID",
        "TRANSACTION_ID", "MERCHANT_ID", "MERCHANT_NAME", "RESELLER_ID",
        "RESELLER_NAME", "PRODUCT_ID", "SP_ID", "SP_NAME", "INSTRUMENT_ID",
        "BANK_ID", "MOBILE_NO", "EMAIL_ID", "UDF1", "UDF2", "UDF3", "UDF4",
        "UDF5", "UDF6", "RECON_STATUS", "NARRATION", "RECON_DATE",
        "COMMISSION_PERCENTAGE", "COMMISSION_FLAT", "TRANSACTION_AMOUNT",
        "POSTING_AMOUNT", "MDR", "GST_ON_MDR", "SURCHARGE",
        "NET_SETTLEMENT_AMOUNT", "MERCHANT_SETTLEMENT_AMOUNT", "REFUND_AMOUNT",
        "BANK_CHARGES", "GST_ON_BANK_CHARGES", "RESELLER_CHARGES",
        "GST_ON_RESELLER_CHARGES", "TOTAL_CHARGES", "NET_PROFIT", "REFUND_TYPE",
        "BANK_REF_NO", "IS_GENERATED", "PAYOUT_REF_NO", "UTR_NO", "INCOMMING_UTR",
        "OUTGOING_UTR", "UTR_UDF", "GENERATED_BY", "MODIFIED_BY", "ArnNo",
        "RefundRequestId", "ReconId", "payout_escrow", "CB_AMOUNT", "CB_Status",
        "MPR_STATUS", "MPR_AMOUNT", "MPR_DATE", "MPR_RRN", "MPR_UDF1", "MPR_UDF2",
        "MPR_UDF3", "MPR_UDF4", "MPR_UDF5", "is_tsp", "SETTLEMENT_CYCLE",
        "PAYOUT_BY", "is_gross", "cycle", "GENERATED_ON", "MODIFIED_ON",
        "SourceFileName"
    ],
}


def enforce_exact_output_schema(df, exact_columns):
    """Reorder/rename output columns to exact requested schema without changing vendor logic."""
    if exact_columns is None:
        return df

    if df is None or not isinstance(df, pd.DataFrame):
        return pd.DataFrame(columns=exact_columns)

    # Build normalized lookup from current columns.
    lookup = {}
    for col in df.columns:
        lookup.setdefault(normalize_col_name(col), col)

    out = pd.DataFrame(index=df.index)
    for exact_col in exact_columns:
        source_col = lookup.get(normalize_col_name(exact_col))
        if source_col is not None:
            out[exact_col] = df[source_col].values
        else:
            out[exact_col] = pd.NA

    return out.reindex(columns=exact_columns)

def reindex_to_schema(df, columns):
    """Keep only the requested columns, in order; add blanks for missing columns."""
    if df is None or not isinstance(df, pd.DataFrame):
        return pd.DataFrame(columns=columns)
    out = df.copy()
    for col in columns:
        if col not in out.columns:
            out[col] = pd.NA
    return out.reindex(columns=columns)

def build_workbook_bytes(excel_data, summary_results, mpr_date):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        for sheet_name, source_vendor_name, template_cols in OUTPUT_SHEET_SPECS:
            if source_vendor_name is None:
                df_to_write = pd.DataFrame(columns=template_cols)
            else:
                df_to_write = excel_data.get(source_vendor_name, pd.DataFrame())
                if sheet_name == "8yes_bank_nb":
                    df_to_write = reindex_to_schema(df_to_write, YES_BANK_COLUMNS)

            exact_cols = EXACT_OUTPUT_SCHEMAS.get(sheet_name)
            if exact_cols is not None:
                df_to_write = enforce_exact_output_schema(df_to_write, exact_cols)

            try:
                df_to_write.to_excel(writer, sheet_name=sheet_name[:31], index=False)
            except Exception:
                pd.DataFrame(df_to_write.astype(str)).to_excel(
                    writer, sheet_name=sheet_name[:31], index=False
                )
    output.seek(0)
    return output.getvalue()

def build_summary_workbook_bytes(summary_df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        summary_df.to_excel(writer, sheet_name="Summary", index=False)
    output.seek(0)
    return output.getvalue()


# =========================================================
# MID MAPPING HELPERS
# =========================================================
#MID_MAPPING_FILE_PATH = r"Z:\OPERATIONS\Kedaresh\Copy Bank Statement MTD MID Mapping Till - 23 Mar 2026.xlsx"
MID_MAPPING_FILE_PATH = r"C:\Users\KedareshPashakanti\Desktop\Bank Statement MTD MID Mapping Till - 24 Mar 2026.xlsx"
MID_MAPPING_SHEET_NAME = "Master - Day"

MID_SP_ALIAS_MAP = {
    "INDIANBANKUPI": ["INDIANBANKUPI", "Indianbank UPI", "Indian Bank UPI", "1INDIANBANKUPI"],
    "HDFC Cards": ["HDFC", "HDFC Cards"],
    "SBI Acquiring": ["SBI Acquiring", "SBI Acq"],
    "SBI NB": ["SBI NB"],
    "ATOM NB": ["Atom NB", "ATOM NB"],
    "HDFC NB": ["HDFC NB"],
    "AXIS NB": ["AXIS Bank NB", "AXIS NB"],
    "YES NB": ["YES Bank NB", "YES NB"],
    "ICICI NB": ["ICICI NB"],
    "HDFC UPI": ["HDFCUPI", "HDFC UPI"],
    "ECMS": ["ECMS"],
    "Worldline NB": ["WorldLine NB", "Worldline NB"],
    "ICICI Cards": ["ICICICards", "ICICI Cards"],
    "PayzApp": ["PayzApp"],
    "1PayecmsHDFC": ["1PayecmsHDFC"],
    "1PayecmsIndianbank": ["1PayecmsIndianbank"],
    "Airtelpay": ["Airtelpay"],
    "Bildesk": ["bildesk","Billdesk", "Bildesk"],
    "MobilewareUPI": ["MobilewareUPI"],
    "Kotak UPI": ["KotakUPI", "Kotak UPI"],
}

def _safe_numeric_sum(series):
    if series is None:
        return 0.0
    cleaned = pd.to_numeric(
        series.astype(str).str.replace(r"[^\d\.\-]", "", regex=True).replace("", pd.NA),
        errors="coerce",
    )
    return float(cleaned.sum(skipna=True) or 0.0)

def load_mid_mapping_master():
    if not os.path.exists(MID_MAPPING_FILE_PATH):
        return None, f"MID Mapping file not found: {MID_MAPPING_FILE_PATH}"

    try:
        raw_df = pd.read_excel(
            MID_MAPPING_FILE_PATH,
            sheet_name=MID_MAPPING_SHEET_NAME,
            header=None,
            dtype=str,
        )
    except Exception as e:
        return None, f"Failed to read MID Mapping file: {e}"

    if raw_df is None or raw_df.empty:
        return None, "MID Mapping file is empty."

    header_row = None
    for i in range(min(20, len(raw_df))):
        row_text = " ".join(map(str, raw_df.iloc[i].values)).lower()
        if "date" in row_text and "sp" in row_text:
            header_row = i
            break

    if header_row is None:
        return None, "Header row not found in MID Mapping file"

    master_df = raw_df.copy()
    master_df.columns = master_df.iloc[header_row]
    master_df = master_df.iloc[header_row + 1 :].reset_index(drop=True)
    master_df.columns = [str(c).replace("\n", " ").strip() for c in master_df.columns]

    if "SP" in master_df.columns:
        master_df = master_df[
            master_df["SP"].notna()
            & (~master_df["SP"].astype(str).str.contains(
                r"total|credit mapped|other credits|running balance", case=False, na=False
            ))
        ].reset_index(drop=True)

    return master_df, None

def build_mid_mapping_comparison(summary_df, selected_mpr_date):
    master_df, err = load_mid_mapping_master()
    if err:
        return None, None, err

    col_lookup = {normalize_col_name(c): c for c in master_df.columns}
    date_col = next((orig for norm, orig in col_lookup.items() if norm == "date"), None)
    sp_col = next((orig for norm, orig in col_lookup.items() if norm == "sp"), None)
    credit_col = next((orig for norm, orig in col_lookup.items() if "creditreceived" in norm), None)
    mpr_col = next((orig for norm, orig in col_lookup.items() if "mpramount" in norm), None)

    if not date_col or not sp_col or not credit_col or not mpr_col:
        return None, None, "MID Mapping sheet is missing one of required columns: Date, SP, Credit Received, MPR Amount"

    work = master_df.copy()
    work[date_col] = pd.to_datetime(work[date_col], errors="coerce", dayfirst=True).dt.date
    work = work[work[date_col] == selected_mpr_date].copy()
    work[credit_col] = pd.to_numeric(work[credit_col].astype(str).str.replace(",", "").str.strip(), errors="coerce")
    work[mpr_col] = pd.to_numeric(work[mpr_col].astype(str).str.replace(",", "").str.strip(), errors="coerce")

    rows = []
    for _, srow in summary_df.iterrows():
        vendor = str(srow["Vendor"])
        total = pd.to_numeric(str(srow["Total"]).replace(",", ""), errors="coerce")
        aliases = MID_SP_ALIAS_MAP.get(vendor, [vendor])
        #alias_norms = {normalize_col_name(a) for a in aliases}
        alias_norms = {
    normalize_col_name(str(a).split("-", 1)[-1])
    for a in aliases
}

        #sp_norm = work[sp_col].astype(str).map(normalize_col_name)
        sp_norm = work[sp_col].astype(str).apply(
    lambda x: normalize_col_name(
        re.sub(r"^\d+\s*-\s*", "", str(x))
        #str(x).split("-", 1)[-1]  # 🔥 REMOVE "1-" PREFIX
    )
)
        matched = work[sp_norm.isin(alias_norms)].copy()

        matched_sp = matched[sp_col].iloc[0] if len(matched) else None
        credit_available = bool(len(matched) > 0 and matched[credit_col].notna().any())
        credit_received = _safe_numeric_sum(matched[credit_col]) if credit_available else None
        mpr_amount_master = _safe_numeric_sum(matched[mpr_col]) if len(matched) > 0 else None

        if not len(matched):
            diff = None
            status = "⚠ Missing SP"
            hint = "SP not found in MID mapping"
        elif not credit_available:
            diff = None
            status = "⚠ Credit Received Not Available"
            hint = "Credit Received amount not available for this SP/date"
        else:
            diff = None if pd.isna(total) or credit_received is None else float(total - credit_received)
            status = "✅ Match" if diff == 0 else "❌ Mismatch"
            if diff == 0:
                hint = "No refund / no action"
            elif diff > 0:
                hint = f"Check refund file / short received by {abs(diff):,.2f}"
            else:
                hint = f"Over received / investigate excess by {abs(diff):,.2f}"

        rows.append({
            "Vendor": vendor,
            "SP": matched_sp if matched_sp else (aliases[0] if aliases else vendor),
            "Total (MPR Amount)": total,
            "Credit Received": credit_received,
            "MPR Amount (MID File)": mpr_amount_master,
            "Refund/Difference": diff,
            "Credit Received Available": "Yes" if credit_available else "No",
            "MID Status": status,
            "Refund Hint": hint,
        })

    comparison_df = pd.DataFrame(rows)
    comparison_df = comparison_df[
        [
            "Vendor",
            "SP",
            "Total (MPR Amount)",
            "Credit Received",
            "MPR Amount (MID File)",
            "Refund/Difference",
            "Credit Received Available",
            "MID Status",
            "Refund Hint",
        ]
    ]
    return comparison_df, work, None

def style_mid_mapping_df(df):
    def row_style(row):
        status = str(row.get("MID Status", ""))
        avail = str(row.get("Credit Received Available", ""))
        if "Match" in status:
            return ["background-color: #d4edda; color: #155724;"] * len(row)
        if "Missing SP" in status or avail == "No":
            return ["background-color: #fff3cd; color: #856404;"] * len(row)
        return ["background-color: #f8d7da; color: #721c24;"] * len(row)

    return (
        df.style
        .apply(row_style, axis=1)
        .format(
            {
                "Total (MPR Amount)": "{:,.2f}",
                "Credit Received": "{:,.2f}",
                "MPR Amount (MID File)": "{:,.2f}",
                "Refund/Difference": "{:,.2f}",
            },
            na_rep="",
        )
    )

def build_mid_mapping_workbook_bytes(comparison_df, filtered_master_df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        comparison_df.to_excel(writer, sheet_name="MID_Comparison", index=False)
        if filtered_master_df is not None:
            filtered_master_df.to_excel(writer, sheet_name="MID_Master_Filtered", index=False)

        workbook = writer.book
        ws = writer.sheets["MID_Comparison"]

        header_fmt = workbook.add_format({
            "bold": True,
            "font_color": "white",
            "bg_color": "#1f4e78",
            "border": 1,
        })
        for col_num, value in enumerate(comparison_df.columns):
            ws.write(0, col_num, value, header_fmt)

        widths = {
            "Vendor": 24,
            "SP": 24,
            "Total (MPR Amount)": 18,
            "Credit Received": 18,
            "MPR Amount (MID File)": 20,
            "Refund/Difference": 18,
            "Credit Received Available": 22,
            "MID Status": 18,
            "Refund Hint": 34,
        }
        for idx, col in enumerate(comparison_df.columns):
            ws.set_column(idx, idx, widths.get(col, 18))

        green_fmt = workbook.add_format({"bg_color": "#d4edda", "font_color": "#155724"})
        red_fmt = workbook.add_format({"bg_color": "#f8d7da", "font_color": "#721c24"})
        yellow_fmt = workbook.add_format({"bg_color": "#fff3cd", "font_color": "#856404"})

        status_col = comparison_df.columns.get_loc("MID Status")
        avail_col = comparison_df.columns.get_loc("Credit Received Available")
        diff_col = comparison_df.columns.get_loc("Refund/Difference")

        # Conditional formatting is done with formulas against the first data row
        def excel_col(n):
            s = ""
            n += 1
            while n:
                n, rem = divmod(n - 1, 26)
                s = chr(65 + rem) + s
            return s

        status_letter = excel_col(status_col)
        avail_letter = excel_col(avail_col)

        ws.conditional_format(
            1, 0, len(comparison_df), len(comparison_df.columns) - 1,
            {"type": "formula", "criteria": f'=${status_letter}2="✅ Match"', "format": green_fmt}
        )
        ws.conditional_format(
            1, 0, len(comparison_df), len(comparison_df.columns) - 1,
            {"type": "formula", "criteria": f'=${status_letter}2="⚠ Missing SP"', "format": yellow_fmt}
        )
        ws.conditional_format(
            1, 0, len(comparison_df), len(comparison_df.columns) - 1,
            {"type": "formula", "criteria": f'=${avail_letter}2="No"', "format": yellow_fmt}
        )
        ws.conditional_format(
            1, diff_col, len(comparison_df), diff_col,
            {"type": "cell", "criteria": ">", "value": 0, "format": red_fmt}
        )
        ws.conditional_format(
            1, diff_col, len(comparison_df), diff_col,
            {"type": "cell", "criteria": "=", "value": 0, "format": green_fmt}
        )

    output.seek(0)
    return output.getvalue()

# =========================================================
# SQL STYLE REPORT (NEW ADDITION)
# =========================================================
SQL_STYLE_REPORT_COLUMNS = [
    "MPR_Date",
    "1Pay_Transaction_ID",
    "Gross_Amt",
    "MSFAndCharges",
    "CR_Amount",
    "SP_Name",
]

SQL_SP_LABELS = {
    "HDFC Cards": "2-HDFC",
    "SBI Acquiring": "3-SBI Acquiring",
    "SBI NB": "4-SBI NB",
    "ATOM NB": "5-Atom NB",
    "HDFC NB": "6-HDFC NB",
    "AXIS NB": "7-AXIS Bank NB",
    "YES NB": "8-YES Bank NB",
    "ICICI NB": "9-ICICI NB",
    "HDFC UPI": "10-HDFCUPI",
    "Worldline NB": "12-WorldLine NB",
    "ICICI Cards": "13-ICICICards",
    "1PayecmsHDFC": "15-1PayecmsHDFC",
    "Bildesk": "18-Billdesk",
    "Kotak UPI": "20-KotakUPI",
    "ATOM NB - Refund": "5-Atom NB - Refund",
}

def _match_col(df, *candidates):
    """Find a column in df by exact/normalized match."""
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return None

    norm_map = {normalize_col_name(c): c for c in df.columns}

    for cand in candidates:
        if cand is None:
            continue
        key = normalize_col_name(cand)
        if key in norm_map:
            return norm_map[key]

    for cand in candidates:
        if cand is None:
            continue
        key = normalize_col_name(cand)
        for norm, orig in norm_map.items():
            if key and key in norm:
                return orig

    return None

def _series_num(df, col_name):
    if col_name is None or col_name not in df.columns:
        return pd.Series([pd.NA] * len(df), index=df.index)
    return to_numeric_series_cleanup(df[col_name])

def _date_filter_df(df, mpr_date_value):
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return df
    if "MPR_Date" not in df.columns:
        return df.copy()
    out = df.copy()
    out["__mpr_date__"] = pd.to_datetime(out["MPR_Date"], errors="coerce").dt.date
    out = out[out["__mpr_date__"] == mpr_date_value].copy()
    out = out.drop(columns=["__mpr_date__"], errors="ignore")
    return out

def _append_report_rows(base_df, cols_map, sp_name, report_rows, extra_filter=None, transform=None):
    if base_df is None or not isinstance(base_df, pd.DataFrame) or base_df.empty:
        return

    df = base_df.copy()
    df = _date_filter_df(df, mpr_date)

    if extra_filter is not None:
        try:
            df = df.loc[extra_filter(df)].copy()
        except Exception:
            return

    if df.empty:
        return

    if transform is not None:
        try:
            df = transform(df)
        except Exception:
            return

    if df.empty:
        return

    report_rows.append(df)

def build_sql_style_report(excel_data, selected_mpr_date):
    """
    Build a SQL-style report from the already processed vendor frames.
    Existing vendor logic remains unchanged; this only reads the outputs.
    """
    if not excel_data:
        return pd.DataFrame(columns=SQL_STYLE_REPORT_COLUMNS), None

    out_frames = []

    def fmt_date(df):
        return pd.Series([selected_mpr_date.strftime("%d-%m-%Y")] * len(df), index=df.index)

    # 2-HDFC
    df = excel_data.get("HDFC Cards")
    if isinstance(df, pd.DataFrame) and not df.empty:
        recfmt = _match_col(df, "REC_FMT", "REC FMT")
        dom = _match_col(df, "DOMESTIC_AMT", "DOMESTIC AMT")
        intl = _match_col(df, "INTNL_AMT", "INTNL AMT")
        msf = _match_col(df, "MSF")
        serv_tax = _match_col(df, "SERV_TAX", "SERV TAX")
        sb = _match_col(df, "SB_Cess", "SB Cess")
        kk = _match_col(df, "KK_Cess", "KK Cess")
        cgst = _match_col(df, "CGST_AMT", "CGST AMT")
        sgst = _match_col(df, "SGST_AMT", "SGST AMT")
        igst = _match_col(df, "IGST_AMT", "IGST AMT")
        ugst = _match_col(df, "UTGST_AMT", "UTGST AMT")
        net = _match_col(df, "Net_Amount", "Net Amount")
        trk = _match_col(df, "MERCHANT_TRACKID", "MERCHANT TRACKID")

        if recfmt and trk and net:
            dfx = df.copy()
            dfx = dfx[dfx[recfmt].astype(str).str.strip().str.upper() == "BAT"].copy()
            if not dfx.empty:
                gross = _series_num(dfx, dom).add(_series_num(dfx, intl), fill_value=0)
                msfchg = (
                    _series_num(dfx, msf).add(_series_num(dfx, serv_tax), fill_value=0)
                    .add(_series_num(dfx, sb), fill_value=0)
                    .add(_series_num(dfx, kk), fill_value=0)
                    .add(_series_num(dfx, cgst), fill_value=0)
                    .add(_series_num(dfx, sgst), fill_value=0)
                    .add(_series_num(dfx, igst), fill_value=0)
                    .add(_series_num(dfx, ugst), fill_value=0)
                )
                out_frames.append(pd.DataFrame({
                    "MPR_Date": fmt_date(dfx),
                    "1Pay_Transaction_ID": dfx[trk].astype(str),
                    "Gross_Amt": gross.round(2),
                    "MSFAndCharges": msfchg.round(2),
                    "CR_Amount": _series_num(dfx, net).round(2),
                    "SP_Name": SQL_SP_LABELS["HDFC Cards"],
                }))

    # 3-SBI Acquiring
    df = excel_data.get("SBI Acquiring")
    if isinstance(df, pd.DataFrame) and not df.empty:
        txn = _match_col(df, "MERCHANT_TXNNO", "MERCHANT TXNNO")
        gross_col = _match_col(df, "GROSS_AMT", "GROSS AMT")
        mdr = _match_col(df, "MDR")
        net = _match_col(df, "NET_AMT", "NET AMT")
        if txn and gross_col and net:
            dfx = _date_filter_df(df, selected_mpr_date)
            if not dfx.empty:
                out_frames.append(pd.DataFrame({
                    "MPR_Date": fmt_date(dfx),
                    "1Pay_Transaction_ID": dfx[txn].astype(str),
                    "Gross_Amt": _series_num(dfx, gross_col).round(2),
                    "MSFAndCharges": (-_series_num(dfx, mdr)).round(2),
                    "CR_Amount": _series_num(dfx, net).round(2),
                    "SP_Name": SQL_SP_LABELS["SBI Acquiring"],
                }))

    # 4-SBI NB
    df = excel_data.get("SBI NB")
    if isinstance(df, pd.DataFrame) and not df.empty:
        txn = _match_col(df, "OnePay_Id", "OnePay Id")
        amt = _match_col(df, "Amt", "Amount")
        if txn and amt:
            dfx = _date_filter_df(df, selected_mpr_date)
            if not dfx.empty:
                out_frames.append(pd.DataFrame({
                    "MPR_Date": fmt_date(dfx),
                    "1Pay_Transaction_ID": dfx[txn].astype(str),
                    "Gross_Amt": _series_num(dfx, amt).round(2),
                    "MSFAndCharges": pd.Series([0.0] * len(dfx), index=dfx.index),
                    "CR_Amount": _series_num(dfx, amt).round(2),
                    "SP_Name": SQL_SP_LABELS["SBI NB"],
                }))

    # 5-Atom NB + refunds
    df = excel_data.get("ATOM NB")
    if isinstance(df, pd.DataFrame) and not df.empty:
        txn = _match_col(df, "Merchant_Txn_ID", "Merchant Txn ID")
        gross_col = _match_col(df, "Gross_Txn_Amount", "Gross Txn Amount")
        chg_col = _match_col(df, "Total_Chargeable", "Total Chargeable")
        net = _match_col(df, "Net_Amount_to_be_Paid", "Net Amount to be Paid")
        state = _match_col(df, "Txn_State", "Txn State")
        if txn and gross_col and chg_col and net:
            dfx = _date_filter_df(df, selected_mpr_date)
            if not dfx.empty:
                state_series = dfx[state].astype(str).str.strip().str.lower() if state else pd.Series([""] * len(dfx), index=dfx.index)
                sale_mask = state_series.eq("sale")
                refund_mask = state_series.isin(["partial refund", "full refund"])

                if sale_mask.any():
                    dfr = dfx.loc[sale_mask].copy()
                    out_frames.append(pd.DataFrame({
                        "MPR_Date": fmt_date(dfr),
                        "1Pay_Transaction_ID": dfr[txn].astype(str),
                        "Gross_Amt": _series_num(dfr, gross_col).round(2),
                        "MSFAndCharges": _series_num(dfr, chg_col).round(2),
                        "CR_Amount": _series_num(dfr, net).round(2),
                        "SP_Name": SQL_SP_LABELS["ATOM NB"],
                    }))
                if refund_mask.any():
                    dfr = dfx.loc[refund_mask].copy()
                    out_frames.append(pd.DataFrame({
                        "MPR_Date": fmt_date(dfr),
                        "1Pay_Transaction_ID": dfr[txn].astype(str),
                        "Gross_Amt": _series_num(dfr, gross_col).round(2),
                        "MSFAndCharges": _series_num(dfr, chg_col).round(2),
                        "CR_Amount": _series_num(dfr, net).round(2),
                        "SP_Name": SQL_SP_LABELS["ATOM NB - Refund"],
                    }))

    # 6-HDFC NB
    df = excel_data.get("HDFC NB")
    if isinstance(df, pd.DataFrame) and not df.empty:
        txn = _match_col(df, "OnePay_Id", "OnePay Id")
        amt = _match_col(df, "Amount")
        resp = _match_col(df, "Response_Code", "Response Code")
        if txn and amt:
            dfx = _date_filter_df(df, selected_mpr_date)
            if resp:
                dfx = dfx[dfx[resp].astype(str).str.strip().isin(["0", "0.0"])].copy()
            if not dfx.empty:
                out_frames.append(pd.DataFrame({
                    "MPR_Date": fmt_date(dfx),
                    "1Pay_Transaction_ID": dfx[txn].astype(str),
                    "Gross_Amt": _series_num(dfx, amt).round(2),
                    "MSFAndCharges": pd.Series([0.0] * len(dfx), index=dfx.index),
                    "CR_Amount": _series_num(dfx, amt).round(2),
                    "SP_Name": SQL_SP_LABELS["HDFC NB"],
                }))

    # 7-AXIS Bank NB
    df = excel_data.get("AXIS NB")
    if isinstance(df, pd.DataFrame) and not df.empty:
        txn = _match_col(df, "ITC_No", "ITC No")
        amt = _match_col(df, "Amount")
        if txn and amt:
            dfx = _date_filter_df(df, selected_mpr_date)
            if not dfx.empty:
                out_frames.append(pd.DataFrame({
                    "MPR_Date": fmt_date(dfx),
                    "1Pay_Transaction_ID": dfx[txn].astype(str),
                    "Gross_Amt": _series_num(dfx, amt).round(2),
                    "MSFAndCharges": pd.Series([0.0] * len(dfx), index=dfx.index),
                    "CR_Amount": _series_num(dfx, amt).round(2),
                    "SP_Name": SQL_SP_LABELS["AXIS NB"],
                }))

    # 8-YES Bank NB
    df = excel_data.get("YES NB")
    if isinstance(df, pd.DataFrame) and not df.empty:
        txn = _match_col(df, "Merchant_Reference", "Merchant Reference")
        amt = _match_col(df, "Amount")
        svc = _match_col(df, "Service_Charges", "Service Charges")
        if txn and amt:
            dfx = _date_filter_df(df, selected_mpr_date)
            if not dfx.empty:
                out_frames.append(pd.DataFrame({
                    "MPR_Date": fmt_date(dfx),
                    "1Pay_Transaction_ID": dfx[txn].astype(str),
                    "Gross_Amt": _series_num(dfx, amt).round(2),
                    "MSFAndCharges": _series_num(dfx, svc).round(2) if svc else pd.Series([0.0] * len(dfx), index=dfx.index),
                    "CR_Amount": _series_num(dfx, amt).round(2),
                    "SP_Name": SQL_SP_LABELS["YES NB"],
                }))

    # 9-ICICI NB
    df = excel_data.get("ICICI NB")
    if isinstance(df, pd.DataFrame) and not df.empty:
        txn = _match_col(df, "OnePay_Id", "OnePay Id")
        amt = _match_col(df, "Amount")
        if txn and amt:
            dfx = _date_filter_df(df, selected_mpr_date)
            if not dfx.empty:
                out_frames.append(pd.DataFrame({
                    "MPR_Date": fmt_date(dfx),
                    "1Pay_Transaction_ID": dfx[txn].astype(str),
                    "Gross_Amt": _series_num(dfx, amt).round(2),
                    "MSFAndCharges": pd.Series([0.0] * len(dfx), index=dfx.index),
                    "CR_Amount": _series_num(dfx, amt).round(2),
                    "SP_Name": SQL_SP_LABELS["ICICI NB"],
                }))

    # 10-HDFCUPI
    df = excel_data.get("HDFC UPI")
    if isinstance(df, pd.DataFrame) and not df.empty:
        txn = _match_col(df, "Order_ID", "Order ID")
        gross_col = _match_col(df, "Transaction_Amount", "Transaction Amount")
        msf = _match_col(df, "MSF_Amount", "MSF Amount")
        cgst = _match_col(df, "CGST_AMT", "CGST AMT")
        sgst = _match_col(df, "SGST_AMT", "SGST AMT")
        igst = _match_col(df, "IGST_AMT", "IGST AMT")
        ugst = _match_col(df, "UTGST_AMT", "UTGST AMT")
        net = _match_col(df, "Net_Amount", "Net Amount")
        crdr = _match_col(df, "CR_DR", "CR_DR", "CR / DR", "CR/DR", "CR DR")
        if txn and gross_col and net:
            dfx = _date_filter_df(df, selected_mpr_date)
            if crdr:
                dfx = dfx[dfx[crdr].astype(str).str.strip().str.upper() == "CR"].copy()
                #dfx = dfx[dfx[crdr].astype(str).str.strip().str.upper().eq("CR")].copy()
            if not dfx.empty:
                msfchg = _series_num(dfx, msf)
                for c in [cgst, sgst, igst, ugst]:
                    if c:
                        msfchg = msfchg.add(_series_num(dfx, c), fill_value=0)
                out_frames.append(pd.DataFrame({
                    "MPR_Date": fmt_date(dfx),
                    "1Pay_Transaction_ID": dfx[txn].astype(str),
                    "Gross_Amt": _series_num(dfx, gross_col).round(2),
                    "MSFAndCharges": msfchg.round(2),
                    "CR_Amount": _series_num(dfx, net).round(2),
                    "SP_Name": SQL_SP_LABELS["HDFC UPI"],
                }))

    # 12-WorldLine NB
    df = excel_data.get("Worldline NB")
    if isinstance(df, pd.DataFrame) and not df.empty:
        txn = _match_col(df, "SM_Transaction_Id", "SM Transaction Id")
        gross_col = _match_col(df, "Total_Amount", "Total Amount")
        chg = _match_col(df, "Charges")
        taxes = _match_col(df, "Taxes")
        net = _match_col(df, "Net_Amount", "Net Amount")
        if txn and gross_col and net:
            dfx = _date_filter_df(df, selected_mpr_date)
            if not dfx.empty:
                msfchg = _series_num(dfx, chg)
                if taxes:
                    msfchg = msfchg.add(_series_num(dfx, taxes), fill_value=0)
                out_frames.append(pd.DataFrame({
                    "MPR_Date": fmt_date(dfx),
                    "1Pay_Transaction_ID": dfx[txn].astype(str),
                    "Gross_Amt": _series_num(dfx, gross_col).round(2),
                    "MSFAndCharges": msfchg.round(2),
                    "CR_Amount": _series_num(dfx, net).round(2),
                    "SP_Name": SQL_SP_LABELS["Worldline NB"],
                }))

    # 13-ICICICards
    df = excel_data.get("ICICI Cards")
    if isinstance(df, pd.DataFrame) and not df.empty:
        txn = _match_col(df, "Order_id", "Order ID")
        gross_col = _match_col(df, "Gross_Amount", "Gross Amount")
        mdr = _match_col(df, "MDR")
        mdr_gst = _match_col(df, "MDR_GST", "MDR GST")
        net = _match_col(df, "Net_Amount", "Net Amount")
        if txn and gross_col and net:
            dfx = _date_filter_df(df, selected_mpr_date)
            if not dfx.empty:
                msfchg = _series_num(dfx, mdr)
                if mdr_gst:
                    msfchg = msfchg.add(_series_num(dfx, mdr_gst), fill_value=0)
                out_frames.append(pd.DataFrame({
                    "MPR_Date": fmt_date(dfx),
                    "1Pay_Transaction_ID": dfx[txn].astype(str),
                    "Gross_Amt": _series_num(dfx, gross_col).round(2),
                    "MSFAndCharges": msfchg.round(2),
                    "CR_Amount": _series_num(dfx, net).round(2),
                    "SP_Name": SQL_SP_LABELS["ICICI Cards"],
                }))

    # 15-1PayecmsHDFC
    df = excel_data.get("1PayecmsHDFC")
    if isinstance(df, pd.DataFrame) and not df.empty:
        txn = _match_col(df, "Reference_No", "Reference No")
        amt = _match_col(df, "Amount")
        if txn and amt:
            dfx = _date_filter_df(df, selected_mpr_date)
            if not dfx.empty:
                out_frames.append(pd.DataFrame({
                    "MPR_Date": fmt_date(dfx),
                    "1Pay_Transaction_ID": dfx[txn].astype(str),
                    "Gross_Amt": _series_num(dfx, amt).round(2),
                    "MSFAndCharges": pd.Series([0.0] * len(dfx), index=dfx.index),
                    "CR_Amount": _series_num(dfx, amt).round(2),
                    "SP_Name": SQL_SP_LABELS["1PayecmsHDFC"],
                }))

    # 18-Billdesk
    df = excel_data.get("Bildesk")
    if isinstance(df, pd.DataFrame) and not df.empty:
        txn = _match_col(df, "Ref_1", "Ref 1")
        gross_col = _match_col(df, "Gross_Amount", "Gross Amount")
        chg = _match_col(df, "Charges")
        gst = _match_col(df, "GST")
        net = _match_col(df, "Net_Amount", "Net Amount")
        if txn and gross_col and net:
            dfx = _date_filter_df(df, selected_mpr_date)
            if not dfx.empty:
                msfchg = _series_num(dfx, chg)
                if gst:
                    msfchg = msfchg.add(_series_num(dfx, gst), fill_value=0)
                out_frames.append(pd.DataFrame({
                    "MPR_Date": fmt_date(dfx),
                    "1Pay_Transaction_ID": dfx[txn].astype(str),
                    "Gross_Amt": _series_num(dfx, gross_col).round(2),
                    "MSFAndCharges": msfchg.round(2),
                    "CR_Amount": _series_num(dfx, net).round(2),
                    "SP_Name": SQL_SP_LABELS["Bildesk"],
                }))

    # 20-KotakUPI
    df = excel_data.get("Kotak UPI")
    if isinstance(df, pd.DataFrame) and not df.empty:
        txn = _match_col(df, "NVL_TSDK_ORDERID_CBS_ORDERID", "NVL TSDK ORDERID CBS ORDERID")
        amt = _match_col(df, "AMOUNT", "Amount")
        mdr = _match_col(df, "MDR_CHARGED", "MDR CHARGED")
        gst = _match_col(df, "GST")
        net = _match_col(df, "NET_AMOUNT", "Net Amount")
        if txn and amt and net:
            dfx = _date_filter_df(df, selected_mpr_date)
            if not dfx.empty:
                msfchg = _series_num(dfx, mdr)
                if gst:
                    msfchg = msfchg.add(_series_num(dfx, gst), fill_value=0)
                out_frames.append(pd.DataFrame({
                    "MPR_Date": fmt_date(dfx),
                    "1Pay_Transaction_ID": dfx[txn].astype(str),
                    "Gross_Amt": _series_num(dfx, amt).round(2),
                    "MSFAndCharges": msfchg.round(2),
                    "CR_Amount": _series_num(dfx, net).round(2),
                    "SP_Name": SQL_SP_LABELS["Kotak UPI"],
                }))

    if not out_frames:
        return pd.DataFrame(columns=SQL_STYLE_REPORT_COLUMNS), None

    report_df = pd.concat(out_frames, ignore_index=True, sort=False)
    report_df = report_df.reindex(columns=SQL_STYLE_REPORT_COLUMNS)
    # 🔥 FINAL FIX (SAFE VERSION)
    if "1Pay_Transaction_ID" in report_df.columns:
     col = report_df["1Pay_Transaction_ID"]

    report_df["1Pay_Transaction_ID"] = (
        col.fillna("")              # null safe
           .astype(str)             # convert to string
           .str.replace("'", "", regex=False)  # remove '
           .str.strip()             # trim spaces
    )

    return report_df, None

def build_sql_style_report_workbook_bytes(report_df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        report_df.to_excel(writer, sheet_name="SQL_Style_Report", index=False)
        workbook = writer.book
        ws = writer.sheets["SQL_Style_Report"]

        header_fmt = workbook.add_format({
            "bold": True,
            "font_color": "white",
            "bg_color": "#1f4e78",
            "border": 1,
        })
        money_fmt = workbook.add_format({"num_format": "#,##0.00"})
        for col_num, value in enumerate(report_df.columns):
            ws.write(0, col_num, value, header_fmt)

        widths = {
            "MPR_Date": 14,
            "1Pay_Transaction_ID": 24,
            "Gross_Amt": 16,
            "MSFAndCharges": 16,
            "CR_Amount": 16,
            "SP_Name": 22,
        }
        for idx, col in enumerate(report_df.columns):
            ws.set_column(idx, idx, widths.get(col, 18))
        # number format for numeric cols
        for col_name in ["Gross_Amt", "MSFAndCharges", "CR_Amount"]:
            if col_name in report_df.columns:
                col_idx = report_df.columns.get_loc(col_name)
                ws.set_column(col_idx, col_idx, widths.get(col_name, 18), money_fmt)

    output.seek(0)
    return output.getvalue()

# =========================================================
# MAIN UI
# =========================================================
zip_file = st.file_uploader("Upload ZIP file", type=["zip"])

summary_results = {}
excel_data = {}

if zip_file:
    try:
        zip_ref = zipfile.ZipFile(zip_file)
        zip_names = zip_ref.namelist()

        st.subheader("Vendor status")

        for vendor in vendors:
            status_text, total, df_out, error_text = process_vendor_files(zip_ref, zip_names, vendor)

            if total is not None and df_out is not None:
                st.success(status_text)
                summary_results[vendor["name"]] = total
                excel_data[vendor["name"]] = df_out
                st.caption(", ".join([str(c) for c in df_out.columns[:20]]))
            else:
                st.error(status_text)

            st.markdown("---")

    except Exception as e:
        st.error(f"ZIP processing failed: {e}")

# =========================================================
# SUMMARY + DOWNLOAD
# =========================================================
# =========================================================
# SUMMARY + DOWNLOAD
# =========================================================
if summary_results:
    st.header("📊 Summary Table")
    summary_df = pd.DataFrame(
        list(summary_results.items()),
        columns=["Vendor", "Total"]
    )

    st.dataframe(summary_df)

    main_xlsx = build_workbook_bytes(excel_data, summary_results, mpr_date)
    summary_xlsx = build_summary_workbook_bytes(summary_df)

    # =========================================================
    # MID MAPPING COMPARISON
    # =========================================================
    st.header("🔍 MID Mapping Compare")
    comparison_df, filtered_mid_df, mid_error = build_mid_mapping_comparison(summary_df, mpr_date)

    if mid_error:
        st.warning(mid_error)
    else:
        st.caption(f"MID Mapping file: {MID_MAPPING_FILE_PATH}")
        st.dataframe(style_mid_mapping_df(comparison_df))

        mid_xlsx = build_mid_mapping_workbook_bytes(comparison_df, filtered_mid_df)

        st.download_button(
            label="Download MID Mapping Compare Excel",
            data=mid_xlsx,
            file_name=f"MID_Mapping_Compare_{mpr_date.strftime('%d %b %Y')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    # =========================================================
    # SQL STYLE REPORT
    # =========================================================
    st.header("📄 SQL Style Report")
    sql_report_df, sql_report_error = build_sql_style_report(excel_data, mpr_date)

    if sql_report_error:
        st.warning(sql_report_error)
    else:
        st.dataframe(sql_report_df)
        sql_report_xlsx = build_sql_style_report_workbook_bytes(sql_report_df)

        st.download_button(
            label="Download SQL Style Report",
            data=sql_report_xlsx,
            file_name=f"SQL_Style_Report_{mpr_date.strftime('%d %b %Y')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    # 🔥 Button 1: Main Excel
    st.download_button(
        label="Download Main Excel",
        data=main_xlsx,
        file_name=f"Upload_Format- {mpr_date.strftime('%d %b %Y')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    # 🔥 Button 2: Summary Excel
    st.download_button(
        label="Download Summary Excel",
        data=summary_xlsx,
        file_name=f"Summary- {mpr_date.strftime('%d %b %Y')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
elif zip_file:
    st.info("No vendor data was processed from the ZIP.")

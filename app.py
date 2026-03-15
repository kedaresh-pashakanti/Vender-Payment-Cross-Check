import streamlit as st
import pandas as pd
import csv
import os

# ==========================================
# PAGE CONFIG
# ==========================================

st.set_page_config(page_title="Vendor Reconciliation System", layout="wide")

st.title("💳 Vendor Payment Cross Check")

st.write("Upload vendor files and system will automatically calculate totals.")

# ==========================================
# DELIMITER DETECTION
# ==========================================

def detect_delimiter(file):

    sample = file.read(2048).decode("utf-8", errors="ignore")

    file.seek(0)

    try:
        dialect = csv.Sniffer().sniff(sample)
        return dialect.delimiter
    except:
        return ","


# ==========================================
# GENERIC FILE READER
# ==========================================

def read_file(file):

    ext = file.name.split(".")[-1].lower()

    if ext == "xlsx":
        df = pd.read_excel(file)

    elif ext == "csv":
        df = pd.read_csv(file)

    elif ext in ["txt", "rpt", "wri"]:

        delimiter = detect_delimiter(file)

        df = pd.read_csv(file, delimiter=delimiter)

    else:
        st.error("Unsupported file type")
        return None

    return df


# ==========================================
# SUM CALCULATION
# ==========================================

def calculate_sum(df, column):

    df[column] = pd.to_numeric(df[column], errors="coerce")

    total = df[column].sum()

    return total


# ==========================================
# VENDOR PROCESSING FUNCTIONS
# ==========================================

def process_hdfc_cards(files):

    df = read_file(files)

    total = calculate_sum(df, "Net_Amount")

    return total


def process_sbi_acquiring(files):

    df = read_file(files)

    total = calculate_sum(df, "NET_AMT")

    return total


def process_sbi_nb(files):

    df = read_file(files)

    total = calculate_sum(df, "Amt")

    return total


def process_atom_nb(files):

    df = read_file(files)

    total = calculate_sum(df, "Net_Amount_to_be_Paid")

    return total


def process_hdfc_nb(files):

    df = read_file(files)

    df = df[df["Response_Code"] == 0]

    total = calculate_sum(df, "Amount")

    return total


def process_axis_nb(files):

    df = read_file(files)

    total = calculate_sum(df, "Amount")

    return total


def process_yes_nb(files):

    df = read_file(files)

    total = calculate_sum(df, "Amount")

    return total


def process_icici_nb(files):

    dfs = []

    for file in files:

        df = read_file(file)

        dfs.append(df)

    merged = pd.concat(dfs)

    total = calculate_sum(merged, "Amount")

    return total


def process_hdfc_upi(files):

    dfs = []

    for file in files:

        df = read_file(file)

        dfs.append(df)

    merged = pd.concat(dfs)

    merged = merged[merged["CR_DR"] == "CR"]

    total = calculate_sum(merged, "Net_Amount")

    return total


def process_worldline(files):

    dfs = []

    for file in files:

        df = read_file(file)

        dfs.append(df)

    merged = pd.concat(dfs)

    total = calculate_sum(merged, "Net_Amount")

    return total


def process_icici_cards(files):

    df = read_file(files)

    total = calculate_sum(df, "Net_Amount")

    return total


def process_bildesk(files):

    df = read_file(files)

    total = calculate_sum(df, "Net_Amount")

    return total


# ==========================================
# UI FOR 12 SERVICE PROVIDERS
# ==========================================

vendors = {
    "HDFC Cards": process_hdfc_cards,
    "SBI Acquiring": process_sbi_acquiring,
    "SBI NB": process_sbi_nb,
    "ATOM NB": process_atom_nb,
    "HDFC NB": process_hdfc_nb,
    "AXIS Bank NB": process_axis_nb,
    "YES Bank NB": process_yes_nb,
    "ICICI NB": process_icici_nb,
    "HDFC UPI": process_hdfc_upi,
    "Worldline NB": process_worldline,
    "ICICI Cards": process_icici_cards,
    "Bildesk": process_bildesk
}


for vendor, func in vendors.items():

    st.subheader(vendor)

    uploaded_files = st.file_uploader(
        f"Upload file(s) for {vendor}",
        accept_multiple_files=True,
        key=vendor
    )

    if uploaded_files:

        try:

            if len(uploaded_files) == 1:
                result = func(uploaded_files[0])
            else:
                result = func(uploaded_files)

            st.success(f"Total Amount = {result}")

        except Exception as e:

            st.error(f"Error processing file: {e}")

    st.divider()
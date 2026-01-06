import pandas as pd
import sqlite3
import sqlparse
import os
from groq import Groq
from sentence_transformers import SentenceTransformer, util

from huggingface_hub import login
login("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")

# Load data and model
embedding_model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
groq_api_key = "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"
client = Groq(api_key=groq_api_key)

DB_FILE = "data_2010_temp.db"
df = pd.read_csv("data_2010.csv", sep=';')
meta_sheets = pd.read_excel("sorular-kodlama.xlsx", sheet_name=None)
sorular_df = meta_sheets["sorular"]
kodlama_df = meta_sheets["kodlama"]

# Initialize DB
if not os.path.exists(DB_FILE):
    conn = sqlite3.connect(DB_FILE)
    df.to_sql("data_2010", conn, index=False, if_exists="replace")
    conn.close()

def get_top_variables_for_topic(topic: str, top_n: int = 5, preselect_n: int = 75):
    # Pre-filter using cosine similarity
    merged = sorular_df[["Variable", "Label", "Measurement Level"]].astype(str).dropna()

    def enrich_label(row):
        var = row["Variable"].strip()
        label = row["Label"].strip()
        level = row["Measurement Level"].strip()
        codes = kodlama_df[kodlama_df["Value"].astype(str).str.strip() == var]
        if not codes.empty:
            sample = codes.head(5)
            mapping = ", ".join(f"{str(x).strip()} → {str(lbl).strip()}" for x, lbl in zip(sample["x"], sample["Label"]))
            return f"{var}: {label} ({level}, codes: {mapping})"
        return f"{var}: {label} ({level})"

    merged["FullLabel"] = merged.apply(enrich_label, axis=1)
    full_labels = merged["FullLabel"].tolist()

    # Cosine similarity to select most likely matches
    label_embeddings = embedding_model.encode(full_labels, convert_to_tensor=True)
    topic_embedding = embedding_model.encode(topic, convert_to_tensor=True)
    cosine_scores = util.cos_sim(topic_embedding, label_embeddings)[0]
    top_indices = cosine_scores.argsort(descending=True)[:preselect_n]

    # Prepare reduced metadata for LLM
    reduced_metadata = "\n".join([merged.iloc[int(i)]["FullLabel"] for i in top_indices])

    prompt = f"""
    You are a data analyst. Given the topic "{topic}" and the following dataset metadata, return the {top_n} most relevant variables (columns) that are useful for analyzing this topic.
    Metadata:
    {reduced_metadata}
    Return a comma-separated list of variable names (e.g., age, income, education_level).
    Only include variables, no extra text.
    """

    response = client.chat.completions.create(
        model="llama3-70b-8192",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2
    )

    var_list = response.choices[0].message.content.strip()
    return [v.strip() for v in var_list.split(",") if v.strip() in sorular_df["Variable"].astype(str).values]


def suggest_questions(topic: str):
    top_vars = get_top_variables_for_topic(topic)

    filtered = sorular_df[sorular_df["Variable"].astype(str).isin(top_vars)]

    if filtered.empty:
        return f"No relevant variables found for topic '{topic}'."

    var_info = [
        (str(row["Variable"]).strip(), str(row["Label"]).strip(), str(row["Measurement Level"]).strip())
        for _, row in filtered.iterrows()
    ]

    metadata_text = "DATA COLUMNS:\n" + "\n".join(
        [f"- {v}: {l} ({lvl})" for v, l, lvl in var_info]
    )

    # Add kodlama mappings
    mapping_section = "\n\nCODE MAPPINGS:\n"
    for var, _, _ in var_info:
        codes = kodlama_df[kodlama_df["Value"].astype(str).str.strip() == var]
        if not codes.empty:
            sample = codes.head(5)
            mappings = "\n".join(f"{str(r['x']).strip()} → {str(r['Label']).strip()}" for _, r in sample.iterrows())
            mapping_section += f"{var}:\n{mappings}\n...\n"

    prompt_suggest = f"""
    You are a data analyst. Given the following metadata and code mappings, suggest 10 diverse, insightful questions to help understand the dataset 'data_2010'.
    These questions should be answerable through SQL queries.
    Metadata:
    {metadata_text}
    {mapping_section}
    Return a numbered list only.
    """

    response = client.chat.completions.create(
        model="llama3-70b-8192",
        messages=[{"role": "user", "content": prompt_suggest}],
        temperature=0.3
    )

    return response.choices[0].message.content.strip()


def get_most_relevant_columns(question: str, top_n: int = 5):
    # Merge variable + label
    merged = sorular_df[["Variable", "Label", "Measurement Level"]].astype(str).dropna()

    # Add code mapping context to label
    def enrich_label(row):
        var = row["Variable"].strip()
        label = row["Label"].strip()
        level = row["Measurement Level"].strip()

        codes = kodlama_df[kodlama_df["Value"].astype(str).str.strip() == var]
        if not codes.empty:
            sample = codes.head(3)
            mapping = ", ".join(f"{str(x).strip()} → {str(lbl).strip()}" for x, lbl in zip(sample["x"], sample["Label"]))
            return f"{label} (codes: {mapping})"
        return label

    merged["FullLabel"] = merged.apply(enrich_label, axis=1)

    # Embedding comparison
    full_labels = merged["FullLabel"].tolist()
    label_embeddings = embedding_model.encode(full_labels, convert_to_tensor=True)
    question_embedding = embedding_model.encode(question, convert_to_tensor=True)

    cosine_scores = util.cos_sim(question_embedding, label_embeddings)[0]
    top_results = cosine_scores.argsort(descending=True)[:top_n]

    columns = []
    for idx in top_results:
        row = merged.iloc[int(idx)]
        var = row["Variable"].strip()
        label = row["Label"].strip()
        level = row["Measurement Level"].strip()
        columns.append((var, label, level))
    return columns


def build_metadata_from_question(question):
    relevant = get_most_relevant_columns(question)
    desc = [f"- {v}: {l} ({lvl})" for v, l, lvl in relevant]

    mapping_section = "\n\nCODE MAPPINGS:\n"
    for var, _, _ in relevant:
        codes = kodlama_df[kodlama_df["Value"].astype(str).str.strip() == var]
        if not codes.empty:
            sample = codes.head(2)
            mappings = "\n".join(f"{str(r['x']).strip()} → {str(r['Label']).strip()}" for _, r in sample.iterrows())
            mapping_section += f"{var}:\n{mappings}\n...\n"

    return "DATA COLUMNS:\n" + "\n".join(desc) + mapping_section


def clean_sql_query(raw_sql):
    if raw_sql.startswith("```") and raw_sql.endswith("```"):
        raw_sql = raw_sql.strip("`").strip()
    raw_sql = raw_sql.replace('COUNT(*)"', 'COUNT(*)').replace('COUNT(*) "', 'COUNT(*) ')
    return sqlparse.format(raw_sql, reindent=True, keyword_case='upper').strip()


def generate_sql(question: str):
    metadata_text = build_metadata_from_question(question)

    prompt_sql = f"""
    You are a data analyst working with a dataset called 'data_2010'. A user has asked a specific question.

    Based on the question and the metadata below, write a correct **SQLite SQL query** that directly answers the question.
    Ensure the query reflects the user’s intent and uses the most relevant columns.

    Always wrap column names in double quotes.

    User question: "{question}"

    Data documentation:
    {metadata_text}

    Return **only** the SQL query. Do not explain anything.
    """

    response = client.chat.completions.create(
        model="llama3-70b-8192",
        messages=[{"role": "user", "content": prompt_sql}],
        temperature=0.2
    )
    return clean_sql_query(response.choices[0].message.content.strip())



def decode_column_values(result_df, column_name):
    column_name = column_name.strip()
    mapping = kodlama_df[kodlama_df["Value"].str.strip() == column_name]
    if mapping.empty:
        print(f"[Warning] No mapping found for column '{column_name}'")
        return result_df
    
    def normalize(val):
        try:
            return str(int(float(val)))
        except:
            return str(val).strip()
        
    code_to_label = {
        normalize(k): str(v).strip() for k, v in zip(mapping["x"], mapping["Label"])
    }
    result_df[column_name] = result_df[column_name].apply(
        lambda x: f"{x} → {code_to_label.get(normalize(x), 'Unknown')}"
    )
    return result_df


def execute_sql(sql_query: str):
    conn = sqlite3.connect(DB_FILE)
    try:
        df = pd.read_sql_query(sql_query, conn)
        for col in df.columns:
            if col in sorular_df["Variable"].astype(str).values:
                df = decode_column_values(df, col)
                break
        return df
    finally:
        conn.close()


def format_sample_data(result_df, max_rows=10):
    sample = result_df.head(max_rows)
    headers = list(sample.columns)
    lines = []
    lines.append(" | ".join(headers))
    lines.append("-" * len(lines[0]))
    for _, row in sample.iterrows():
        lines.append(" | ".join(str(row[h]) for h in headers))
    return "\n".join(lines)

def summarize_answer(question: str, result_df: pd.DataFrame):
    if result_df.empty:
        return "Sonuç bulunamadı."

    summary_table = format_sample_data(result_df, max_rows=10)

    prompt_answer = f"""
    You are a helpful data analyst assistant. Given the following summary table of query results, provide a concise explanation highlighting important patterns, trends, or anomalies. Do not just repeat the data, but explain what it means in context.
    The summary should be precise and informative. It should be designed for a non-technical audience.
    User question: "{question}"

    Query result summary:
    {summary_table}
    """

    response = client.chat.completions.create(
        model="llama3-70b-8192",
        messages=[{"role": "user", "content": prompt_answer}],
        temperature=0.3
    )

    return response.choices[0].message.content.strip()



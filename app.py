import streamlit as st
import pandas as pd
import llm_backend as llm

st.set_page_config(page_title="📊 Talk to your data", layout="wide")
st.title("📊 Talk to your data")

# Konu seçimi
topic_options = ["politik", "sosyal", "sağlık", "ekonomik", "demografik", "eğitim", "çevre"]
selected_topic = st.selectbox("Pick a topic:", topic_options)

if st.button("💡 Suggest questions"):
    with st.spinner("Generating..."):
        suggestions = llm.suggest_questions(selected_topic)
        st.markdown(suggestions)

user_question = st.text_input("Ask a question about the dataset:")

if st.button("▶ Run SQL Query"):
    if user_question.strip() == "":
        st.warning("Please enter a question.")
    else:
        with st.spinner("Generating..."):
            sql_query = llm.generate_sql(user_question)
            st.subheader("SQL query")
            st.code(sql_query, language="sql")

        with st.spinner("Running query..."):
            try:
                result_df = llm.execute_sql(sql_query)
                st.subheader("Results")
                st.dataframe(result_df.style.set_properties(**{'text-align': 'left'}), use_container_width=True)
            except Exception as e:
                st.error(f"SQL error: {e}")
                result_df = pd.DataFrame()

        if not result_df.empty:
            with st.spinner("Summarizing..."):
                summary = llm.summarize_answer(user_question, result_df)
                st.subheader("Summary")
                st.write(summary)

# Dataseek Deep Research Agent - Text-to-SQL Pipeline

This repository contains the submission for the Dataseek Text-to-SQL Agent assignment. It implements a highly robust, compliance-aware LLM agent (`DatabaseAgent`) designed to translate natural language requests into complex PostgreSQL queries while strictly adhering to dynamic business rules and database schemas.

## 🚀 Core Architectural Highlights

To ensure enterprise-grade reliability and mitigate common LLM hallucinations (such as over-selecting columns, hallucinating composite JOIN keys, or failing strict PostgreSQL typing), this agent implements an advanced **Data-Aware Actor-Critic Architecture**:

1. **Zero-Shot Draft Generation (Actor):** The agent ingests heterogeneous data sources (Schema DDL, JSON column definitions, and JSONL business rules) to generate an initial SQL draft and a compliance decision (`execute` or `reject`).
2. **Data-Aware Execution Feedback:** Before returning the result, the agent actively attempts to execute the draft query against the local PostgreSQL database using `psycopg2`.
3. **Self-Reflection & Auto-Correction (Critic):** - **If the query fails** (e.g., `ENUM` type casting errors), the exact PostgreSQL traceback is fed back to the LLM to autonomously debug and fix the query.
   - **If the query succeeds**, the LLM samples the execution results (row count and top 5 rows) to perform a sanity check. It verifies logical correctness (e.g., percentages $\le$ 100), checks for zero-row over-filtering anomalies, and ensures the exact `SELECT` column order requested by the user is respected.
4. **Bulletproof JSON Extraction:** A regex-based fallback parser guarantees stable JSON output, immune to unexpected LLM conversational text or markdown formatting.

## 🛠️ Prerequisites

- **Python 3.9+**
- **PostgreSQL**: A local instance running on port `5432` with the `labor_certification` database loaded.
- **Anthropic API Key**: Access to the Claude API (default model used is `claude-haiku-4-5-20251001` for optimal latency/performance balance during dual-pass review).

## ⚙️ Installation & Setup

1. Clone or extract the repository.
2. Install the required Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Export your Anthropic API key as an environment variable:
    ```bash
    export ANTHROPIC_API_KEY="your_api_key_here"
    ```
## 🖥️ Usage & Testing
To run the agent and evaluate it against the provided test tasks, simply execute:  
    ```bash
    python database_agent.py
    ```

What to expect during execution:
Task 1 (The Happy Path): The agent will generate a complex 4-table JOIN query, dynamically correct any column ordering or filtering issues during the Review phase, and print the beautifully formatted tabular results directly from the database.

Task 2 (The Reject Trap): The agent will successfully catch the mass-deletion violation defined in the knowledge_base.jsonl (Rule 64), immediately returning an "action": "reject" response with an empty SQL string, safeguarding the database.


## 📁 Repository Structure
database_agent.py: The core agent logic featuring the Actor-Critic pipeline.

requirements.txt: Minimal dependency file (anthropic, psycopg2-binary).

data/: Directory containing the database schema, column meanings, and the business rule knowledge base.

example_task_*.json: Test case configurations.

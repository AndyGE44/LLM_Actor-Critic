import psycopg2
import os
import json
import time
import re
from anthropic import Anthropic

class DatabaseAgent:
    def __init__(self, model_name: str, task: dict[str, str]):
        """Initialize the database agent."""
        self.model_name = model_name
        self.task = task
        
        # Connect to the local PostgreSQL database
        self.conn = psycopg2.connect(
            dbname="labor_certification",
            user=os.getenv("USER"),
            host="localhost",
            port="5432"
        )
        
        # Extract user request
        self.user_query = task.get("request", "")
        
        # Load database column meanings (JSON)
        self.column_meanings = self._load_file(task.get("column_meaning", ""), is_jsonl=False)
        
        # Load business rules knowledge base (JSONL)
        self.knowledge_base = self._load_file(task.get("knowledge_base", ""), is_jsonl=True)
        
        # Attempt to load Schema DDL file
        schema_path = "data/labor_certification_applications/labor_certification_applications_schema.txt"
        self.schema_ddl = self._load_file(schema_path, is_jsonl=False, is_raw_text=True)
        
        # Initialize Claude client
        self.client = Anthropic()

    def _load_file(self, filepath: str, is_jsonl: bool = False, is_raw_text: bool = False) -> str:
        """Ultimate universal file reading pipeline, resilient to formatting errors."""
        if not filepath or not os.path.exists(filepath):
            return ""
            
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                if is_raw_text:
                    return f.read()
                elif is_jsonl:
                    data = []
                    for line in f:
                        if line.strip(): # Ignore empty lines
                            data.append(json.loads(line.strip()))
                    return json.dumps(data, indent=2)
                else:
                    return json.dumps(json.load(f), indent=2)
        except Exception as e:
            print(f"Error reading {filepath}: {e}")
            return ""

    def _build_system_prompt(self) -> str:
        """System prompt with strict risk control constraints and SQL generation specifications."""
        return f"""You are an elite PostgreSQL architect and a strict compliance officer.

### 1. DATABASE SCHEMA & DDL
{self.schema_ddl}

### 2. COLUMN MEANINGS
{self.column_meanings}

### 3. COMPLIANCE KNOWLEDGE BASE (BUSINESS RULES)
{self.knowledge_base}

### SQL GENERATION GUIDELINES (STRICT):
1. SELECT ORDER: Select EXACTLY and ONLY the columns explicitly requested, and in the EXACT ORDER they were requested in the prompt.
2. JOIN CONDITIONS (CRITICAL): Pay strict attention to composite keys defined in the schema. For example, when joining `cases` and `employer`, you MUST use both `c.homefirm = e.corphandle AND c.homezip = e.zipref`.
3. Do NOT add WHERE conditions that are not explicitly stated.
4. When counting records across JOINs, ALWAYS use `COUNT(DISTINCT primary_key)`.
5. STRING MATCHING: For case-insensitive matching on text, you MUST use `LOWER(column) = 'value'` (e.g., `LOWER(c.statustag) = 'certified'`). Do NOT use IN clauses with mixed cases.
6. POSTGRESQL TYPING: Do NOT use `LOWER()` on ENUM columns. Match them exactly (e.g., `c.h1bdep = 'Yes'`).
7. ORDERING: Default to ordering by the most important derived metric DESCENDING (e.g., success rate), followed by volume/counts DESCENDING.


### YOUR DIRECTIVE:
- Rule 1 (COMPLIANCE CHECK): If the request violates ANY rule in the Knowledge Base, you MUST reject it immediately.
- Rule 2 (SQL GENERATION): If 100% compliant, generate a valid, optimized PostgreSQL query.

### OUTPUT FORMAT:
You must respond with ONLY a raw, valid JSON object. Do NOT include markdown code blocks.
{{
  "action": "execute", 
  "sql": "SELECT * FROM..." 
}}
"""
    def handle_request(self) -> dict[str, str]:
        """Actor-Critic architecture with real database error feedback."""
        start_time = time.time()
        system_prompt = self._build_system_prompt()
        
        try:
            # ==========================================
            # Round 1: Actor generates draft (Draft Generation)
            # ==========================================
            draft_response = self.client.messages.create(
                model=self.model_name,
                max_tokens=2048,
                temperature=0.0,
                system=system_prompt,
                messages=[{"role": "user", "content": f"User Request: {self.user_query}"}]
            )
            
            draft_output = draft_response.content[0].text.strip()
            draft_match = re.search(r'\{[\s\S]*\}', draft_output)
            draft_json_str = draft_match.group(0) if draft_match else draft_output
            draft_dict = json.loads(draft_json_str)
            
            if draft_dict.get("action") == "reject":
                action = "reject"
                sql = ""
            else:
                # ==========================================
                # Round 2: Database dry run and Critic review
                # ==========================================
                draft_sql = draft_dict.get("sql", "")
                db_error = None
                sample_data = ""
                row_count = 0
                
                if draft_sql:
                    try:
                        cursor = self.conn.cursor()
                        # Actually execute SQL (using the original here, without EXPLAIN, because we need to see real data)
                        cursor.execute(draft_sql)
                        records = cursor.fetchall()
                        row_count = len(records)
                        
                        # To prevent token explosion, only take up to the first 5 rows as a sample for the model
                        sample_data = str(records[:5]) 
                        cursor.close()
                    except Exception as pg_err:
                        self.conn.rollback() 
                        db_error = str(pg_err).strip()
                
                # Dynamically build Review prompt
                if db_error:
                    # Error occurred: focus on fixing the bug
                    review_instruction = (
                        "Here is the DRAFT SQL generated for the user request:\n"
                        f"```sql\n{draft_sql}\n```\n\n"
                        "I attempted to run this SQL, but it threw the following error:\n"
                        f"ERROR: {db_error}\n\n"
                        "Fix the DRAFT SQL to resolve this database error. Ensure correct typing. Output ONLY the final JSON object."
                    )
                else:
                    # No error: enable advanced [Data-Aware Review]
                    review_instruction = (
                        "Here is the DRAFT SQL generated for the user request:\n"
                        f"```sql\n{draft_sql}\n```\n\n"
                        "I executed this SQL and it successfully returned data.\n"
                        f"Total rows returned: {row_count}\n"
                        f"Sample of the first 5 rows: {sample_data}\n\n"
                        "CRITICAL DATA-AWARE REVIEW TASK:\n"
                        "1. ZERO ROWS: If Total rows = 0, you over-filtered. Fix WHERE clauses, composite JOIN keys (e.g. cases and employer need BOTH homefirm and homezip), or string cases.\n"
                        "2. LOGIC CHECK: Do percentages exceed 100? If so, fix the aggregate math.\n"
                        "3. EXACT COLUMN ORDER (CRUCIAL): The user explicitly asked for 'employer name, attorney email...'. Your SELECT clause MUST output employer FIRST and email SECOND. If your draft did `SELECT lawmail, corphandle`, you MUST swap them to `SELECT corphandle, lawmail`!\n"
                        "4. ORDER BY: Ensure sorting by the calculated rate DESC, then count DESC.\n\n"
                        "Fix the DRAFT SQL if the data reveals any logical or ordering flaws. Output ONLY the final JSON object with 'action' and 'sql' keys."
                    )
                
                review_response = self.client.messages.create(
                    model=self.model_name,
                    max_tokens=2048,
                    temperature=0.0,
                    system=system_prompt,
                    messages=[
                        {"role": "user", "content": f"User Request: {self.user_query}"},
                        {"role": "assistant", "content": draft_output},
                        {"role": "user", "content": review_instruction}
                    ]
                )
                
                final_output = review_response.content[0].text.strip()
                final_match = re.search(r'\{[\s\S]*\}', final_output)
                final_json_str = final_match.group(0) if final_match else final_output
                final_dict = json.loads(final_json_str)
                
                action = final_dict.get("action", "execute")
                sql = final_dict.get("sql", "")
                
        except Exception as e:
            print(f"LLM Reasoning Error: {e}")
            action = "reject"
            sql = ""
            
        end_time = time.time()
        elapsed_seconds = round(end_time - start_time, 2)
        
        return {
            "action": action,
            "sql": sql,
            "elapsed_seconds": elapsed_seconds
        }

def main():
    """Test environment entry point."""
    # To prevent 404 errors due to incorrect model names, I've standardized it to standard Haiku; adjust as needed.
    target_model = "claude-haiku-4-5-20251001"
    
    # ---------------- Test Task 1 (Normal query and closed-loop result validation) ----------------
    print("="*80)
    print("TESTING TASK 1: THE HAPPY PATH (Actor-Critic Enabled)")
    print("="*80)
    
    with open('example_task_1.json', 'r') as f:
        task1 = json.load(f)
        
    agent1 = DatabaseAgent(model_name=target_model, task=task1)
    result1 = agent1.handle_request()
    
    print("\n[Agent Decision for Task 1]")
    print(json.dumps(result1, indent=2))
    
    # Execute and validate the generated SQL
    if result1.get("action") == "execute" and result1.get("sql"):
        print("\n--- Executing the generated SQL in the PostgreSQL database for validation ---")
        try:
            cursor = agent1.conn.cursor()
            cursor.execute(result1["sql"])
            records = cursor.fetchall()
            
            print(f"\nSuccessfully queried {len(records)} results! The first 20 records are as follows:\n")
            print(f"{'Employer':<40} | {'Email':<35} | {'Cases':<8} | {'Cert':<8} | {'Rate %'}")
            print("-" * 110)
            
            for row in records[:20]:
                employer = str(row[0])[:38] if len(row) > 0 else ""
                email = str(row[1])[:33] if len(row) > 1 else ""
                total = str(row[2]) if len(row) > 2 else ""
                cert = str(row[3]) if len(row) > 3 else ""
                rate = str(row[4]) if len(row) > 4 else ""
                print(f"{employer:<40} | {email:<35} | {total:<8} | {cert:<8} | {rate}")
                
            if len(records) > 20:
                print(f"\n... and {len(records) - 20} more results")
            cursor.close()
        except Exception as e:
            print(f"\nSQL execution failed in the database: {e}")

    # ---------------- Test Task 2 (Trap interception validation) ----------------
    print("\n" + "="*80)
    print("TESTING TASK 2: THE REJECT TRAP")
    print("="*80)
    
    if os.path.exists('example_task_2.json'):
        with open('example_task_2.json', 'r') as f:
            task2 = json.load(f)
            
        agent2 = DatabaseAgent(model_name=target_model, task=task2)
        result2 = agent2.handle_request()
        
        print("\n[Agent Decision for Task 2]")
        print(json.dumps(result2, indent=2))
        
    # Clean up connections
    if hasattr(agent1, 'conn') and agent1.conn:
        agent1.conn.close()

if __name__ == "__main__":
    main()
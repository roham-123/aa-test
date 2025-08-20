#!/usr/bin/env python3
"""
AA Poll Survey Chatbot
Natural language interface to the survey database using Azure OpenAI.
Enhanced with conversation memory for follow-up questions.
"""

import logging
import json

from openai import AzureOpenAI
import mysql.connector

from azure_ai_config import AZURE_OPENAI_CONFIG
from db_config import DB_CONFIG

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SurveyChatbot:
    
    def __init__(self):
        """Initialize the chatbot with Azure OpenAI and database connections."""
        # Initialize Azure OpenAI client
        self.client = AzureOpenAI(
            api_key=AZURE_OPENAI_CONFIG["api_key"],
            api_version=AZURE_OPENAI_CONFIG["api_version"],
            azure_endpoint=AZURE_OPENAI_CONFIG["azure_endpoint"]
        )
        self.deployment_name = AZURE_OPENAI_CONFIG["deployment_name"]
        
        # Database connection
        self.db_config = DB_CONFIG
        self.schema_context = self._get_database_schema()
        
        # Load prompts from external file
        self.prompts = self._load_prompts()
        
        # Conversation memory
        self.conversation_history = []
        self.max_history_length = 5  # Keep last 5 interactions
        
        logger.info("🤖 Survey Chatbot with conversation memory initialized successfully!")
    
    def _load_prompts(self):
        """Load all prompts from the external prompts.txt file."""
        try:
            with open("prompts.txt", "r", encoding="utf-8") as f:
                content = f.read()
            
            prompts = {}
            
            # Split by the section dividers
            sections = content.split("# ========================================")
            
            # Process sections: odd indices are headers, even indices are content
            for i in range(1, len(sections), 2):  # Start from 1, step by 2 to get headers
                if i < len(sections):
                    header_section = sections[i]
                    content_section = sections[i+1] if (i+1) < len(sections) else ""
                    
                    # Check what type of section this is from the header
                    if "INTENT ANALYSIS - SYSTEM MESSAGE" in header_section:
                        prompts["intent_system"] = content_section.strip()
                    elif "INTENT ANALYSIS - USER PROMPT" in header_section:
                        prompts["intent_user"] = content_section.strip()
                    elif "SYNTHESIS - SYSTEM MESSAGE" in header_section:
                        prompts["synthesis_system"] = content_section.strip()
                    elif "SYNTHESIS - USER PROMPT" in header_section:
                        prompts["synthesis_user"] = content_section.strip()
            
            logger.info(f"📝 Loaded {len(prompts)} prompt sections from prompts.txt")
            for key, value in prompts.items():
                logger.info(f"  - {key}: {len(value)} characters")
            return prompts
            
        except Exception as e:
            logger.error(f"Error loading prompts from file: {e}")
            raise ValueError(f"Failed to load prompts.txt: {e}. The chatbot requires external prompts to function.")
    
    def _get_database_schema(self):
        """Get database schema information to provide context to the AI."""
        try:
            conn = mysql.connector.connect(**self.db_config)
            cursor = conn.cursor()
            
            schema_info = []
            
            # Get table structures
            tables = ['surveys', 'survey_questions', 'answer_options', 'demographics', 'demographic_responses', 'p1_responses']
            
            for table in tables:
                cursor.execute(f"DESCRIBE {table}")
                columns = cursor.fetchall()
                
                schema_info.append(f"\n--- {table.upper()} TABLE ---")
                for col in columns:
                    schema_info.append(f"{col[0]} {col[1]} {col[2]} {col[3]} {col[4]} {col[5]}")
            
            # Add essential data context (simplified for clean architecture)
            schema_info.append("\n--- KEY DATA CONTEXT ---")
            schema_info.append("Survey IDs: AA-MMYYYY format (e.g., AA-042020)")
            schema_info.append("Question numbers: Q1, Q2... (main), QD1, QD2... (demographics)")
            schema_info.append("Demographics: QD1=Age, QD2=Gender, QD3=Region, QD4=Social grade")
            schema_info.append("Response data: p1_responses.cnt (counts), p1_responses.pct (percentages)")
            schema_info.append("Total survey data: Use WHERE item_label = 'Total' for survey totals")
            schema_info.append("Demographic data: Individual records by item_label (demographic values)")
            
            cursor.close()
            conn.close()
            
            return "\n".join(schema_info)
            
        except Exception as e:
            logger.error(f"Error getting database schema: {e}")
            return "Database schema unavailable"
    
    def execute_sql(self, sql_query):
        """Execute SQL query and return results."""
        try:
            conn = mysql.connector.connect(**self.db_config)
            cursor = conn.cursor()
            
            logger.info(f"🔍 Executing SQL: {sql_query}")
            cursor.execute(sql_query)
            
            # Get column names
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            
            # Fetch results
            results = cursor.fetchall()
            
            # Convert to list of dictionaries for better readability
            formatted_results = []
            for row in results:
                formatted_results.append(dict(zip(columns, row)))
            
            cursor.close()
            conn.close()
            
            return formatted_results, None
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"SQL execution error: {error_msg}")
            return [], error_msg

    def clear_conversation_history(self):
        """Clear conversation history to start fresh."""
        self.conversation_history = []
        logger.info("🧹 Conversation history cleared")

    def analyze_intent_and_plan(self, user_question):
        """Analyze user intent and create a data exploration strategy."""
        try:
            context_info = ""
            if self.conversation_history:
                context_info = f"\nConversation context: {json.dumps(self.conversation_history[-2:], indent=2)}"
            
            # Use external prompt - REQUIRED 
            if not self.prompts.get("intent_user"):
                raise ValueError("Intent analysis prompt not found in prompts.txt. Please ensure prompts.txt is properly loaded.")
            
            analysis_prompt = self.prompts["intent_user"].format(
                user_question=user_question,
                context_info=context_info,
                schema_context=self.schema_context
            )

            # Use external system message - REQUIRED 
            if not self.prompts.get("intent_system"):
                raise ValueError("Intent system prompt not found in prompts.txt. Please ensure prompts.txt is properly loaded.")
            
            system_message = self.prompts["intent_system"]
            
            response = self.client.chat.completions.create(
                model=self.deployment_name,
                messages=[
                    {"role": "system", "content": system_message},
                    {"role": "user", "content": analysis_prompt}
                ],
                temperature=0.3,
                max_tokens=1000
            )
            
            plan_json = response.choices[0].message.content.strip()
            logger.info(f"🧠 Raw response: {plan_json[:200]}...")  # Show first 200 chars for debugging
            
            # Clean and parse the JSON plan
            # Remove markdown code blocks if present
            plan_json = plan_json.strip()
            if plan_json.startswith('```json'):
                plan_json = plan_json[7:]  # Remove ```json
            if plan_json.startswith('```'):
                plan_json = plan_json[3:]  # Remove ``` 
            if plan_json.endswith('```'):
                plan_json = plan_json[:-3]  # Remove ```
            plan_json = plan_json.strip()
            
            logger.info(f"🧠 Cleaned JSON: {plan_json[:200]}...")  # Show cleaned version
            
            plan = json.loads(plan_json)
            logger.info(f"🎯 Parsed intent: {plan.get('intent', 'Unknown')}")
            return plan
            
        except Exception as e:
            logger.error(f"Error in intent analysis: {e}")
            # Improved fallback - extract keywords from the question
            import re
            
            # Extract meaningful words from the question
            words = re.findall(r'\b[a-zA-Z]{3,}\b', user_question.lower())
            # Remove common words
            stop_words = {'the', 'and', 'are', 'for', 'with', 'how', 'what', 'where', 'when', 'why', 'who', 'does', 'did', 'can', 'could', 'would', 'should', 'will', 'respondents', 'people'}
            keywords = [word for word in words if word not in stop_words][:5]  # Take top 5 meaningful words
            
            if not keywords:
                keywords = [user_question]
            
            return {
                "intent": f"Find information about {' and '.join(keywords)} in survey data",
                "topic_keywords": keywords,
                "exploration_strategy": "Search for relevant questions using extracted keywords and provide comprehensive analysis",
                "queries_needed": [
                    {"purpose": "Search for relevant questions", "type": "search_questions", "description": f"Find questions containing {keywords}"},
                    {"purpose": "Get response data", "type": "get_responses", "description": "Analyze response patterns"},
                    {"purpose": "Demographic analysis", "type": "analyze_demographics", "description": "Break down by demographics"}
                ],
                "expected_insights": f"Provide insights about {' and '.join(keywords)} from survey responses"
            }

    def execute_exploration_plan(self, plan, user_question):
        """Execute the data exploration plan with multiple strategic queries."""
        all_results = []
        
        try:
            logger.info(f"🔍 Executing exploration plan: {plan['intent']}")
            
            # Step 1: Find relevant questions using topic keywords
            if any(query["type"] == "search_questions" for query in plan["queries_needed"]):
                logger.info("📋 Searching for relevant questions...")
                topic_keywords = plan.get("topic_keywords", [user_question])
                
                # Build flexible search conditions
                search_conditions = []
                for keyword in topic_keywords[:3]:  # Limit to top 3 keywords
                    search_conditions.append(f"sq.question_text LIKE '%{keyword}%'")
                
                search_sql = f"""
                SELECT DISTINCT sq.question_id, sq.question_number, sq.question_part, 
                       sq.question_text, sq.base_description, s.year, s.month, s.survey_id
                FROM survey_questions sq
                JOIN surveys s ON sq.survey_id = s.survey_id
                WHERE ({' OR '.join(search_conditions)})
                ORDER BY s.year DESC, s.month DESC, sq.question_number, sq.question_part
                LIMIT 20
                """
                
                questions_results, error = self.execute_sql(search_sql)
                if not error and questions_results:
                    all_results.append({
                        "type": "relevant_questions",
                        "purpose": "Questions found related to the topic",
                        "data": questions_results,
                        "sql": search_sql
                    })
                    logger.info(f"📋 Found {len(questions_results)} relevant questions")
            
            # Step 2: Get response data for found questions
            if all_results and any(query["type"] == "get_responses" for query in plan["queries_needed"]):
                logger.info("📊 Getting response data...")
                question_ids = [str(q["question_id"]) for q in all_results[0]["data"][:5]]  # Top 5 questions
                
                if question_ids:
                    responses_sql = f"""
                    SELECT sq.question_text, sq.question_number, sq.question_part, 
                           s.year, s.month, ao.option_text,
                           pr.cnt as total_responses,
                           pr.pct as avg_percentage
                    FROM p1_responses pr
                    JOIN survey_questions sq ON pr.question_id = sq.question_id
                    JOIN answer_options ao ON pr.option_id = ao.option_id
                    JOIN surveys s ON pr.survey_id = s.survey_id
                    WHERE pr.question_id IN ({','.join(question_ids)})
                          AND pr.item_label = 'Total'
                    ORDER BY pr.cnt DESC
                    LIMIT 50
                    """
                    
                    responses_results, error = self.execute_sql(responses_sql)
                    if not error and responses_results:
                        all_results.append({
                            "type": "response_data",
                            "purpose": "Response counts and percentages for relevant questions",
                            "data": responses_results,
                            "sql": responses_sql
                        })
                        logger.info(f"📊 Found {len(responses_results)} response records")
            
            # Step 3: Get demographic breakdowns if requested
            if all_results and any(query["type"] == "analyze_demographics" for query in plan["queries_needed"]):
                logger.info("👥 Getting demographic breakdowns...")
                question_ids = [str(q["question_id"]) for q in all_results[0]["data"][:3]]  # Top 3 questions
                
                if question_ids:
                    demo_sql = f"""
                    SELECT sq.question_text, sq.question_number, sq.question_part,
                           s.year, s.month, d.demo_code, pr.item_label as demographic_value,
                           ao.option_text, pr.cnt as responses, pr.pct as percentage
                    FROM p1_responses pr
                    JOIN survey_questions sq ON pr.question_id = sq.question_id
                    JOIN answer_options ao ON pr.option_id = ao.option_id
                    JOIN demographics d ON pr.demo_id = d.demo_id
                    JOIN surveys s ON pr.survey_id = s.survey_id
                    WHERE pr.question_id IN ({','.join(question_ids)}) 
                          AND pr.demo_id IS NOT NULL
                    ORDER BY pr.cnt DESC
                    LIMIT 100
                    """
                    
                    demo_results, error = self.execute_sql(demo_sql)
                    if not error and demo_results:
                        all_results.append({
                            "type": "demographic_data",
                            "purpose": "Demographic breakdowns for the questions",
                            "data": demo_results,
                            "sql": demo_sql
                        })
                        logger.info(f"👥 Found {len(demo_results)} demographic records")
            
            return all_results
            
        except Exception as e:
            logger.error(f"Error executing exploration plan: {e}")
            return all_results

    def synthesize_insights(self, user_question, plan, exploration_results):
        """Synthesize insights from multiple data sources using GPT-4o's reasoning."""
        try:
            if not exploration_results:
                return "❌ I couldn't find any relevant data for your question. Could you try rephrasing it or asking about a different topic?"
            
            # Prepare data summary for analysis
            data_summary = {
                "user_question": user_question,
                "analysis_plan": plan,
                "data_found": []
            }
            
            for result in exploration_results:
                data_summary["data_found"].append({
                    "type": result["type"],
                    "purpose": result["purpose"],
                    "record_count": len(result["data"]),
                    "sample_data": result["data"][:5],  # First 5 records as sample
                    "all_data": result["data"],  # Include all data for citation purposes
                    "sql_used": result["sql"]
                })
            
            # Use external prompt - REQUIRED 
            if not self.prompts.get("synthesis_user"):
                raise ValueError("Synthesis prompt not found in prompts.txt. Please ensure prompts.txt is properly loaded.")
            
            synthesis_prompt = self.prompts["synthesis_user"].format(
                user_question=user_question,
                plan_intent=plan["intent"],
                data_summary=json.dumps(data_summary, indent=2, default=str)
            )

            # Use external system message - REQUIRED 
            if not self.prompts.get("synthesis_system"):
                raise ValueError("Synthesis system prompt not found in prompts.txt. Please ensure prompts.txt is properly loaded.")
            
            synthesis_system_message = self.prompts["synthesis_system"]
            
            response = self.client.chat.completions.create(
                model=self.deployment_name,
                messages=[
                    {"role": "system", "content": synthesis_system_message},
                    {"role": "user", "content": synthesis_prompt}
                ],
                temperature=0.4,
                max_tokens=1500
            )
            
            analysis = response.choices[0].message.content.strip()
            logger.info("🧠 Generated comprehensive analysis")
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error in insight synthesis: {e}")
            # Fallback to basic summary
            total_records = sum(len(r["data"]) for r in exploration_results)
            return f"I found {total_records} relevant data points for your question about '{user_question}'. The analysis covered {len(exploration_results)} different aspects of the data."

    def chat(self, user_question):
        """Main chat function with intelligent reasoning and comprehensive analysis."""
        logger.info(f"💬 User question: {user_question}")
        
        try:
            # Step 1: Analyze intent and create exploration plan
            analysis_plan = self.analyze_intent_and_plan(user_question)
            logger.info(f"🎯 Intent: {analysis_plan['intent']}")
            
            # Step 2: Execute comprehensive data exploration
            exploration_results = self.execute_exploration_plan(analysis_plan, user_question)
            
            if not exploration_results:
                return f"❌ I couldn't find relevant data for your question: '{user_question}'. Try asking about specific survey questions, demographics, or topics that might be covered in the surveys."
            
            # Step 3: Synthesize intelligent insights
            comprehensive_response = self.synthesize_insights(user_question, analysis_plan, exploration_results)
            
            # Step 4: Add to conversation history with rich context
            conversation_entry = {
                "question": user_question,
                "intent": analysis_plan["intent"],
                "exploration_results": len(exploration_results),
                "total_data_points": sum(len(r["data"]) for r in exploration_results),
                "response": comprehensive_response
            }
            
            self.conversation_history.append(conversation_entry)
            if len(self.conversation_history) > self.max_history_length:
                self.conversation_history.pop(0)
            
            # Step 5: Add follow-up suggestions for deeper analysis
            if exploration_results:
                comprehensive_response += "\n\n💡 Want to dive deeper? Ask about specific demographics, time periods, or related topics for more detailed insights!"
            
            return comprehensive_response
            
        except Exception as e:
            logger.error(f"Error in chat processing: {e}")
            return f"I encountered an error while analyzing your question. Please try rephrasing it or asking something different."


 
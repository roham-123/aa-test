"""Azure OpenAI configuration for the survey chatbot."""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Azure OpenAI Configuration
AZURE_OPENAI_CONFIG = {
    "api_key": os.getenv("AZURE_OPENAI_KEY"),
    "api_version": os.getenv("AZURE_OPENAI_VERSION", "2025-01-01-preview"),
    "azure_endpoint": os.getenv("AZURE_OPENAI_ENDPOINT", "https://aa-poll-ai.cognitiveservices.azure.com/"),
    "deployment_name": os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o")
}

# Validate required credentials
if not AZURE_OPENAI_CONFIG["api_key"]:
    raise ValueError("AZURE_OPENAI_KEY environment variable is required but not set") 
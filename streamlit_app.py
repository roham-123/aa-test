#!/usr/bin/env python3
"""
AA Poll Survey Chatbot - Streamlit Web Interface
Clean and modern UI using Streamlit's native components.
"""

import streamlit as st
import json
from datetime import datetime
import traceback

# Import the chatbot class
from azure_openai.survey_chatbot import SurveyChatbot

# Page configuration
st.set_page_config(
    page_title="AA Poll Survey Chatbot",
    page_icon="🤖",
    layout="centered",
    initial_sidebar_state="collapsed"
)

# Custom CSS to make it look like modern LLMs
st.markdown("""
<style>
    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    
    /* Remove padding and margins */
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
        max-width: 48rem;
    }
    
    /* Center the chat */
    .main .block-container {
        padding-left: 2rem;
        padding-right: 2rem;
    }
    
    /* Style the chat input to look like ChatGPT */
    .stChatInput > div > div > div {
        border-radius: 1.5rem;
        border: 1px solid #d1d5db;
        background-color: white;
    }
    
    /* Style chat messages to look more like ChatGPT */
    .stChatMessage {
        padding: 1rem 0;
    }
    
    /* Make assistant messages have subtle background like ChatGPT */
    .stChatMessage[data-testid="assistant"] {
        background-color: #f7f7f8;
        margin-left: -2rem;
        margin-right: -2rem;
        padding-left: 2rem;
        padding-right: 2rem;
    }
    
    /* Style the title area */
    h1 {
        text-align: center;
        font-weight: 600;
        margin-bottom: 0.5rem;
    }
    
    .title-subtitle {
        text-align: center;
        color: #6b7280;
        margin-bottom: 2rem;
        font-size: 0.9rem;
    }
</style>
""", unsafe_allow_html=True)

def initialize_session_state():
    """Initialize session state variables."""
    if 'chatbot' not in st.session_state:
        st.session_state.chatbot = None
        st.session_state.chatbot_initialized = False
        st.session_state.initialization_error = None
    
    if 'messages' not in st.session_state:
        st.session_state.messages = []
    
    if 'conversation_count' not in st.session_state:
        st.session_state.conversation_count = 0
    
    if 'total_queries' not in st.session_state:
        st.session_state.total_queries = 0

def initialize_chatbot():
    """Initialize the chatbot instance."""
    if not st.session_state.chatbot_initialized:
        try:
            with st.spinner("Initializing Survey Chatbot... 🤖"):
                st.session_state.chatbot = SurveyChatbot()
                st.session_state.chatbot_initialized = True
                st.session_state.initialization_error = None
                st.success("✅ Chatbot initialized successfully!")
        except Exception as e:
            st.session_state.initialization_error = str(e)
            st.error(f"❌ Failed to initialize chatbot: {e}")
            st.error("Please check your configuration files and database connection.")
            return False
    return True


def clear_conversation():
    """Clear the conversation history."""
    st.session_state.messages = []
    if st.session_state.chatbot:
        st.session_state.chatbot.clear_conversation_history()
    st.session_state.conversation_count += 1
    st.rerun()

def export_conversation():
    """Export conversation to JSON format."""
    if st.session_state.messages:
        conversation_data = {
            "export_timestamp": datetime.now().isoformat(),
            "total_messages": len(st.session_state.messages),
            "conversation": st.session_state.messages
        }
        return json.dumps(conversation_data, indent=2, default=str)
    return None

def show_controls_menu():
    """Show controls in an expander at the top, ChatGPT style."""
    with st.expander("⚙️ Controls", expanded=False):
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("🧹 Clear Chat", use_container_width=True):
                clear_conversation()
                st.rerun()
        
        with col2:
            if st.session_state.messages:
                conversation_json = export_conversation()
                if conversation_json:
                    st.download_button(
                        label="📥 Export",
                        data=conversation_json,
                        file_name=f"chat_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                        mime="application/json",
                        use_container_width=True
                    )
        
        with col3:
            if st.session_state.chatbot_initialized:
                st.button("🤖 Ready", disabled=True, use_container_width=True, type="primary")
            else:
                st.button("⚠️ Error", disabled=True, use_container_width=True, type="secondary")

def main_chat_interface():
    """Main chat interface"""
    st.title("Survey Data Assistant")
    st.markdown('<div class="title-subtitle">Ask me anything about the survey data</div>', unsafe_allow_html=True)
    
    # Initialize chatbot
    if not initialize_chatbot():
        st.stop()
    
    # Show controls at the top
    show_controls_menu()
    
    # Main chat container - no extra containers, just pure chat
    if st.session_state.messages:
        for message in st.session_state.messages:
            if message["role"] == "error":
                with st.chat_message("assistant"):
                    st.error(message["content"])
            else:
                with st.chat_message(message["role"]):
                    st.markdown(message["content"])
    else:
        st.markdown("""
        <div style='text-align: center; padding: 2rem; color: #6b7280;'>
            <h3>How can I help you today?</h3>
            <p>Ask me about survey responses, demographics, trends, or any specific questions in your dataset.</p>
        </div>
        """, unsafe_allow_html=True)
    
    # Chat input at the bottom 
    user_input = st.chat_input("Message Survey Assistant...", key="chat_input")
    
    if user_input:
        process_user_input(user_input)

def process_user_input(user_input):
    """Process user input and get chatbot response."""
    # Add user message
    timestamp = datetime.now().strftime("%H:%M:%S")
    st.session_state.messages.append({
        "role": "user",
        "content": user_input,
        "timestamp": timestamp
    })
    st.session_state.total_queries += 1
    
    # Check for clear commands and redirect to button functionality
    if user_input.lower().strip() in ['clear', 'reset', 'start over']:
        clear_conversation()
        return
    
    # Get response from chatbot
    try:
        with st.spinner("🤔 Thinking..."):
            response = st.session_state.chatbot.chat(user_input)
        
        # Add bot response
        response_timestamp = datetime.now().strftime("%H:%M:%S")
        st.session_state.messages.append({
            "role": "assistant",
            "content": response,
            "timestamp": response_timestamp
        })
        
    except Exception as e:
        error_msg = f"Sorry, I encountered an error: {str(e)}"
        st.session_state.messages.append({
            "role": "error",
            "content": error_msg,
            "timestamp": datetime.now().strftime("%H:%M:%S")
        })
        
        # Log detailed error for debugging
        st.error("Detailed error information:")
        st.code(traceback.format_exc())
    
    # Rerun to update the interface
    st.rerun()

def main():
    """Main application function."""
    # Initialize session state
    initialize_session_state()
    
    main_chat_interface()

if __name__ == "__main__":
    main()
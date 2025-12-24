# Personal Assistant

Sabi is a personal companion AI assistant designed as a conversational interface with a unique personality. She lives with Logi as personal companion with specific behavioral traits and communication style.

## Project Overview

This project implements a complete RAG (Retrieval-Augmented Generation) pipeline with a FastAPI-based web interface. The system consists of:

- **Data Pipeline**: ETL (Extract-Transform-Load) pipeline for indexing various data sources
- **Web Application**: FastAPI server with WebSocket support for real-time conversations
- **Vector Search**: Elasticsearch-based retrieval system for contextual responses
- **LLM Integration**: LiteLLM integration with Groq for intelligent responses

## Quick Start

### Prerequisites

1. Python 3.12+ with virtual environment
2. Elasticsearch running on `localhost:9200`
3. Config file set as `CONFIG` environment variable
4. Groq API key set as `GROQ_API_KEY` environment variable
5. Install 'txtai' as a submodule and make it editable mode.

### Setup

```bash
# Activate virtual environment (linux)
source venv/bin/activate

# Install dependencies (if requirements.txt exists)
pip install -r requirements.txt
```

### Running the Application

#### a) Start the Web Application

```bash (linux)
source venv/bin/activate
python3 app/main.py
```

The application will start on `http://localhost:8001` with WebSocket endpoint at `/ws/chat`.

#### b) Test the Data Pipeline

```bash (linux)
source venv/bin/activate
python3 pipeline/test_all.py
```

This runs the complete ETL pipeline: extraction → transformation → loading.

## Architecture

### a) Indexing Pipeline (ETL)

The indexing pipeline follows a modular ETL architecture:

```
Data Sources → Connectors → Extractors → Transformers → Loaders → Elasticsearch
```

**Components:**

1. **Connectors** (`pipeline/connectors/`)
   - Base interface for connecting to external services
   - Implementations: Elasticsearch, Gmail, PostgreSQL
   - Manages connection lifecycle and authentication

2. **Extractors** (`pipeline/extractors/`)
   - Pulls raw data from connected sources
   - Each extractor works with a specific connector type
   - Returns structured data as dictionaries

3. **Transformers** (`pipeline/transformers/`)
   - Processes and cleans extracted data
   - Document transformers for text processing
   - Tabular transformers for structured data
   - Applies normalization, filtering, and formatting

4. **Loaders** (`pipeline/loaders/`)
   - Loads transformed data into Elasticsearch
   - Handles embedding generation and vector indexing
   - Manages batch operations and error handling

**Flow:**
1. `ExtractorManager.run_all_extractions()` - Pull data from all sources
2. `TransformerRunner.run_all_transformers()` - Process and clean data
3. `LoaderRunner.run_all_loaders()` - Index data in Elasticsearch with embeddings

### b) Retrieval Pipeline (Chat Application)

The retrieval pipeline handles user queries through a conversational flow:

```
User Query → Retrieval → Context → Memory → Prompt → LLM → Response
```

**Flow Details:**

1. **User Input** (`app/router/websocket_router.py`)
   - WebSocket receives user message
   - Creates session context

2. **Context Retrieval** (`app/services/retriever_service.py`)
   - Searches Elasticsearch for relevant documents
   - Returns top-k results as context
   - Handles embedding similarity search

3. **Memory Management** (`app/services/memory_service.py`)
   - Maintains conversation history per session
   - Stores user/assistant message pairs
   - Implements session timeout and cleanup

4. **Prompt Building** (`app/services/prompt_service.py`)
   - Combines system prompt, context, and history
   - Applies personality-specific instructions
   - Formats for LLM consumption

5. **LLM Generation** (`app/services/llm_service.py`)
   - Calls Groq API via LiteLLM
   - Generates response based on prompt
   - Handles API errors and retries

6. **Response Delivery**
   - Stores response in memory
   - Sends back via WebSocket
   - Maintains conversation state

**Key Services:**
- `Chat.service` - Main orchestrator for message processing
- `RetrieverService` - Elasticsearch vector search
- `MemoryService` - Session and conversation management
- `PromptService` - Dynamic prompt construction
- `LLMService` - LLM API integration

## Configuration

The application is configured via `app/config/app.yml`:

- **Elasticsearch**: Connection settings and index name
- **LLM**: Groq API configuration and model settings
- **WebSocket**: Host and port settings
- **Chat**: Memory limits and session timeouts
- **System Prompt**: Personality and behavior definitions for Sabi

## Development

The codebase follows SOLID principles with:
- Abstract base classes for extensibility
- Factory patterns for component creation
- Registry patterns for dynamic loading
- Clear separation of concerns

All Python files compile successfully and the core dependencies (FastAPI, uvicorn, yaml) are available in the virtual environment.

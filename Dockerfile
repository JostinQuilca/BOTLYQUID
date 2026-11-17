FROM python:3.11-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends build-essential && rm -rf /var/lib/apt/lists/*
COPY pyproject.toml .
RUN pip install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cpu && \
    pip install --no-cache-dir .
COPY . .
RUN python -c "from kronos_engine import Kronos, KronosTokenizer; KronosTokenizer.from_pretrained('NeoQuasar/Kronos-Tokenizer-2k'); Kronos.from_pretrained('NeoQuasar/Kronos-mini')"
CMD ["python", "main.py"]

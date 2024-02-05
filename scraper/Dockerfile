
FROM python:3.9
WORKDIR /app

# Create and activate a virtual environment
RUN python -m venv venv && \
    /bin/bash -c "source venv/bin/activate"
COPY . .
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "main.py"]

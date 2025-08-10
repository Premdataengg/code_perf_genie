# Use Apache Spark base image
FROM apache/spark:3.5.6-python3

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Switch to root for installation
USER root

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies with user permissions
RUN pip install --no-cache-dir --user -r requirements.txt

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p logs data/generated

# Set permissions
RUN chmod +x main.py

# Expose Spark UI port
EXPOSE 4040

# Set default command
CMD ["python3", "main.py"]

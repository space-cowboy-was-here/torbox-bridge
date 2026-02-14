FROM python:3.10-slim

# Set the working directory
WORKDIR /app

# Copy the requirements file
COPY requirements.txt .

# Install dependencies
RUN apt-get update && apt-get install -y aria2 && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY app /app

# Expose the port the app runs on
EXPOSE 5051

# Command to run the application
CMD ["python", "main.py"]
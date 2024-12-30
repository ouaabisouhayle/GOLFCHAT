FROM python:3.13-slim

WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Expose port 8080 to the outside once the container has launched
EXPOSE 8080

# Set environment variables
ENV FLASK_APP=chatbotapi.py
ENV FLASK_RUN_HOST=0.0.0.0
ENV FLASK_RUN_PORT=8080

# Define the command to run your app using Flask's built-in server
CMD ["flask", "run"]

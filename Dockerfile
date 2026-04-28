# Stage 1: Build the application
FROM maven:3.9.6-eclipse-temurin-8 AS build
WORKDIR /app

# Copy pom and resolve dependencies to cache them
COPY pom.xml .
RUN mvn dependency:go-offline -B

# Copy source and build the fat JAR
COPY src ./src
RUN mvn clean package -DskipTests

# Stage 2: Runtime environment
FROM eclipse-temurin:8-jre
WORKDIR /app

# Copy the generated fat JAR from the build stage
COPY --from=build /app/target/nasa-log-etl-1.0-SNAPSHOT.jar app.jar

# Create the data directory for logs
RUN mkdir -p /app/data

# Run the application directly
ENTRYPOINT ["java", "-jar", "app.jar"]
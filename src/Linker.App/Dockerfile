# Base runtime
FROM mcr.microsoft.com/dotnet/runtime:9.0 AS base
WORKDIR /app

# Build stage
FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src

# Fix the paths for COPY
COPY src/Linker.App/Linker.App.csproj Linker.App/
COPY src/Linker/Linker.csproj Linker/
RUN dotnet restore Linker.App/Linker.App.csproj

# Copy all source code
COPY src/ .

# Build
WORKDIR /src/Linker.App
RUN dotnet build Linker.App.csproj -c $BUILD_CONFIGURATION -o /app/build

# Publish
FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish Linker.App.csproj -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

# Final runtime image
FROM mcr.microsoft.com/dotnet/runtime:9.0 AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "Linker.App.dll"]

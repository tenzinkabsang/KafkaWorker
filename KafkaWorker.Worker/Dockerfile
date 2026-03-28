FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS runtime

ARG BIN_PATH

WORKDIR /app

COPY $BIN_PATH .

ENTRYPOINT ["dotnet", "KafkaWorker.Worker.dll"]

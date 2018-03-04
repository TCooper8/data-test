FROM microsoft/dotnet:2.0-sdk AS build-env

WORKDIR /app

COPY *.fsproj ./
RUN dotnet restore

COPY . ./
RUN dotnet publish -c Release -o out

FROM microsoft/dotnet:2.0-runtime
WORKDIR /app
COPY --from=build-env /app/out ./
CMD [ "dotnet", "data-service.dll" ]
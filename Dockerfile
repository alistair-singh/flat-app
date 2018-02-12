FROM microsoft/dotnet:sdk AS build
WORKDIR /app

# copy csproj and restore as distinct layers
COPY *.sln .
COPY src/*.csproj ./src/
COPY tests/*.csproj ./tests/
RUN dotnet restore

# copy and build everything else
COPY src/. ./src/
COPY tests/. ./tests/

RUN dotnet build

FROM build AS testrunner
WORKDIR /app/tests
ENTRYPOINT ["dotnet", "test","--logger:trx"]

FROM build AS test
WORKDIR /app/tests
RUN dotnet test

FROM test AS publish
WORKDIR /app/src
RUN dotnet publish -o out

FROM microsoft/dotnet:runtime AS runtime
WORKDIR /app
COPY --from=publish /app/src/out ./
ENTRYPOINT ["dotnet", "app.dll"]

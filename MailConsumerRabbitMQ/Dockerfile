# Temel imaj olarak .NET SDK kullan
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build

# Çalýþma dizinini ayarla
WORKDIR /app

# Proje dosyasýný kopyala ve baðýmlýlýklarý yükle
COPY *.csproj ./
RUN dotnet restore

# Uygulamayý derle
COPY . ./
RUN dotnet publish -c Release -o /out

# Çalýþtýrma için temel imaj olarak .NET Runtime kullans
FROM mcr.microsoft.com/dotnet/aspnet:8.0

# Çalýþma dizinini ayarlan
WORKDIR /app

# Yayýnlanan dosyalarý kopyalad
COPY --from=build /out .

# Uygulamayý çalýþtýrs
ENTRYPOINT ["dotnet", "MailConsumerRabbitMQ.dll"]

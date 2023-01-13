SET var=%cd%

docker run ^
  --env-file .\config\env.list ^
  --volume %var%\config\google_credentials.json:/srv/google_key.json:ro ^
  --mount type=bind,source=%var%\scripts,target=/srv/scripts ^
  --rm ^
  crypto_tax_report

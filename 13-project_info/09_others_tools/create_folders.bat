@echo off
setlocal enabledelayedexpansion

for /f "tokens=*" %%a in (table_names.txt) do (
	mkdir "%%a"
)

echo create folder complete!
pause
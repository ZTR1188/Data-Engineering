@echo off
set BASE_URL=http://localhost:5000/api/v1

curl -s %BASE_URL%/health | python -m json.tool
echo.

curl -s %BASE_URL%/sensors | python -m json.tool
echo.

curl -s %BASE_URL%/sensors/temperature/latest | python -m json.tool
echo.

curl -s "%BASE_URL%/sensors/temperature/stats?days=3" | python -m json.tool
echo.

curl -s "%BASE_URL%/anomalies?sensor=temperature&limit=5" | python -m json.tool
echo.

curl -s -X POST %BASE_URL%/readings -H "Content-Type: application/json" -d "{\"sensor\":\"temperature\",\"value\":29.5}" | python -m json.tool
echo.

curl -s "%BASE_URL%/sensors/temperature/stats?days=abc" | python -m json.tool
echo.

pause
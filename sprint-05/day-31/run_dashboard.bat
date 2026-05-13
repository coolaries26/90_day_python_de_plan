@echo off
echo ============================================
echo  DVD Rental Analytics Dashboard
echo  Python DE Journey
echo ============================================
echo.

cd /d C:\90_day_python_de_plan

echo Activating virtual environment...
call .venv\Scripts\activate

echo Starting Streamlit dashboard...
echo Open: http://localhost:8501
echo Press Ctrl+C to stop.
echo.

streamlit run sprint-05/day-31/app.py --server.port 8501 --browser.gatherUsageStats false

pause
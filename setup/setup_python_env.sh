pip install --upgrade pip
 if [ ! -d /home/mborges/FlightPrices/setup/FlightPrices ]; then
     cd /home/mborges/FlightPrices/setup/
     python -m venv FlightPrices
fi
source FlightPrices/bin/activate
pip install -r requirements.txt

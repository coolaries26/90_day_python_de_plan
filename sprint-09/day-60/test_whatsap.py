import pywhatkit    #type: ignore
from datetime import datetime, time,timedelta
import pyautogui    #type: ignore
now=datetime.now()
send_time=now+timedelta(minutes=1)
print(f"scheduling for {send_time.hour:02d}:{send_time.minute:02d}...")
#pywhatkit.sendwhatmsg("+917019906123","Hello how are you",send_time.hour,send_time.minute)
pywhatkit.sendwhatmsg_instantly("+919986743999", "Hello how are you")
#time.sleep(10)  # wait for WhatsApp Web to load
pyautogui.press("enter")  # simulate pressing Enter
wait_time=10,   # seconds to wait before pressing Enter
tab_close=True  # close the tab after sending

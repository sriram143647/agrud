import pandas as pd
import requests
session = requests.Session()

token = 'Bearer eyJraWQiOiJ4d1Y2T3o1ZEtSeEgxNld4cW1ueDV0alI1TllJckgxaU1KVGlQcEh5am8wPSIsImFsZyI6IlJTMjU2In0.eyJzdWIiOiIzbDduaG0wNnNpbjg4cnUxZ2pzMDZudm5odiIsInRva2VuX3VzZSI6ImFjY2VzcyIsInNjb3BlIjoiaHR0cHM6XC9cL2RldmVsb3Blci5jZnJhcmVzZWFyY2guY29tXC9hcGkiLCJhdXRoX3RpbWUiOjE2Mzg4NzA5MzAsImlzcyI6Imh0dHBzOlwvXC9jb2duaXRvLWlkcC51cy1lYXN0LTEuYW1hem9uYXdzLmNvbVwvdXMtZWFzdC0xX1pkWVJkMUl4SCIsImV4cCI6MTYzODg3NDUzMCwiaWF0IjoxNjM4ODcwOTMwLCJ2ZXJzaW9uIjoyLCJqdGkiOiI0MTY4NjNkMy01OGRlLTQyMzMtOTI4OC03MDhkMzk4ODk1ZjQiLCJjbGllbnRfaWQiOiIzbDduaG0wNnNpbjg4cnUxZ2pzMDZudm5odiJ9.VtqOpatWN5QhfIV5i_mr5TVuDU6-DXmINUvnynOQwwAtvw0zIkFZ2Ig0_2e0leZO1p4KarVOoK68GTfr2fFPDpHfAXe5xyzPad14eAw3bsadyAID_FqfUrqQifzEmRrJhOAyPStApasJAXnaXItSw7jNZXJxRDM4yXpZ4eWDABm_QMqS5KrMicZweRG5gMz2w9XZZAn6UMAi1Wt8-m2tmlYAL4EcjDw-IabICGeHWd10UFbZfnDc7mXdy-N5GMId3eIeOYvh0NCTc8smA3ImItgfvE4QzaA3auOA_jbw8DjF98CDTFuKPPwG41wFP_H_DHgkjs5D2nxOVGgTVp00kg'
headers = {
  'accept': '*/*',
  'Authorization': token,
  'x-api-key': 'z18plRKX0s6BEZuOB34WQZHEDUlgBI1aXXrwP6Ha'
}

mainDf = pd.DataFrame()
opts = ['strong_sell', 'sell', 'hold', 'buy', 'strong_buy']
for p in opts:
    api_url = f"https://api.cfraresearch.com/equity/quant/universe?recommendation={p}"
    print(api_url)
    result = session.get(api_url, headers=headers).json()
    df = pd.DataFrame(result['result'])
    df['recommendation'] = p
    mainDf = mainDf.append(df)
mainDf.to_csv("api2_temp_data.csv", index = False)
#data filter
df1 = pd.read_csv(r'D:\\sriram\\agrud\\cfra\\CFRA-API\\ticker_data.csv')
# df2 = pd.read_csv('api2_temp_data.csv')
df2 = mainDf
df3 = df1.merge(df2, how = "left", left_on = ["source_symbol", "source_exchange"], right_on=["ticker", "exchange_code"])
df3 = df3.dropna()
df3.to_csv('api2.csv', index=False)

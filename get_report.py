import datetime
import requests
import json
import pandas
import numpy
import io
import time
import re
from pytz import timezone
import streamlit as st
import pydeck as pdk

st.set_page_config(layout="wide")

FILE_BUFFER = io.BytesIO()
#CLAIM_SECRETS = st.secrets["CLAIM_SECRETS"]
API_URL = st.secrets["API_URL"]
SECRETS_MAP = {"Juntoz": 0}
CLIENTS_MAP = {0: "Juntoz"}

statuses = {
    'delivered': {'type': '4. delivered', 'state': 'in progress'},
    'pickuped': {'type': '3. pickuped', 'state': 'in progress'},
    'returning': {'type': '3. pickuped', 'state': 'in progress'},
    'cancelled_by_taxi': {'type': 'X. cancelled', 'state': 'final'},
    'delivery_arrived': {'type': '3. pickuped', 'state': 'in progress'},
    'cancelled': {'type': 'X. cancelled', 'state': 'final'},
    'performer_lookup': {'type': '1. created', 'state': 'in progress'},
    'performer_found': {'type': '2. assigned', 'state': 'in progress'},
    'performer_draft': {'type': '1. created', 'state': 'in progress'},
    'returned': {'type': 'R. returned', 'state': 'in progress'},
    'returned_finish': {'type': 'R. returned', 'state': 'final'},
    'performer_not_found': {'type': 'X. cancelled', 'state': 'final'},
    'return_arrived': {'type': '3. pickuped', 'state': 'in progress'},
    'delivered_finish': {'type': '4. delivered', 'state': 'final'},
    'failed': {'type': 'X. cancelled', 'state': 'final'},
    'accepted': {'type': '1. created', 'state': 'in progress'},
    'new': {'type': '1. created', 'state': 'in progress'},
    'pickup_arrived': {'type': '2. assigned', 'state': 'in progress'},
    'estimating_failed': {'type': 'X. cancelled', 'state': 'final'},
    'cancelled_with_payment': {'type': 'X. cancelled', 'state': 'final'}
}
def get_claims(secret, date_from, date_to, cursor=0):
    url = API_URL
  
    timezone_offset = "-05:00"
    payload = json.dumps({
        "created_from": f"{date_from}T00:00:00{timezone_offset}",
        "created_to": f"{date_to}T23:59:59{timezone_offset}",
        "limit": 1000,
        "cursor": cursor
    }) if cursor == 0 else json.dumps({"cursor": cursor})

    headers = {
        'Content-Type': 'application/json',
        'Accept-Language': 'en',
        'Authorization': f"Bearer {secret}"
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    claims = json.loads(response.text)
    cursor = None
    try:
        cursor = claims['cursor']
        print(f"CURSOR: {cursor}")
    except:
        print("LAST PAGE PROCESSED")
    return claims['claims'], cursor


def get_report(CLAIM_SECRETS, period, start_, end_) -> pandas.DataFrame:
    client_timezone = "America/Lima"
    today = datetime.datetime.now(timezone(client_timezone))
    date_from_offset = datetime.datetime.fromisoformat(start_).astimezone(
        timezone(client_timezone)) - datetime.timedelta(days=2)
    date_from = date_from_offset.strftime("%Y-%m-%d")
    date_to = end_
    today = today.strftime("%Y-%m-%d")
    report = []
    for idx, secret in enumerate(CLAIM_SECRETS):
      try:
          claims, cursor = get_claims(secret, date_from, date_to)
      except:
          break
      while cursor:
          new_page_claims, cursor = get_claims(secret, date_from, date_to, cursor)
          claims = claims + new_page_claims
      for claim in claims:
          try:
              claim_from_time = claim['same_day_data']['delivery_interval']['from']
          except:
              continue
          report_client = CLIENTS_MAP[idx]
          cutoff_time = datetime.datetime.fromisoformat(claim_from_time).astimezone(timezone(client_timezone))
          cutoff_date = cutoff_time.strftime("%Y-%m-%d")
          if not start_:
              if cutoff_date != today:
                  continue
          report_cutoff = cutoff_time.strftime("%Y-%m-%d %H:%M")
          try:
              report_client_id = claim['route_points'][1]['external_order_id'].replace("\t", " ")
          except:
              report_client_id = "Sin order ID"
          report_claim_id = claim['id']
          report_pickup_address = claim['route_points'][0]['address']['fullname']
          report_pod_point_id = str(claim['route_points'][1]['id'])
          report_receiver_address = claim['route_points'][1]['address']['fullname']
          report_receiver_phone = claim['route_points'][1]['contact']['phone']
          report_receiver_name = claim['route_points'][1]['contact']['name']
          try:
              report_comment = claim['route_points'][1]['address']['comment']
          except:
              report_comment = "Sin comment"
          report_status = claim['status']
          report_status_time = datetime.datetime.strptime(claim['updated_ts'],"%Y-%m-%dT%H:%M:%S.%f%z").astimezone(
        timezone(client_timezone))
          report_store_name = claim['route_points'][0]['contact']['name']
          report_longitude = claim['route_points'][1]['address']['coordinates'][0]
          report_latitude = claim['route_points'][1]['address']['coordinates'][1]
          report_store_longitude = claim['route_points'][0]['address']['coordinates'][0]
          report_store_latitude = claim['route_points'][0]['address']['coordinates'][1]
          try: 
              report_status_type = statuses[report_status]['type']
              report_status_is_final = statuses[report_status]['state']
          except:
              report_status_type = "?. other"
              report_status_is_final = "unknown"
          try:
              report_courier_name = claim['performer_info']['courier_name']
          except:
              report_courier_name = "Sin mensajero asignado"
          try:
              report_return_reason = str(claim['route_points'][1]['return_reasons'])
          except:
              report_return_reason = "No return reasons"
          try:
              report_autocancel_reason = claim['autocancel_reason']
          except:
              report_autocancel_reason = "Sin cancel reasons"
          try:
              report_route_id = claim['route_id']
          except:
              report_route_id = "No route"
          try:
              report_price_of_goods = 0
              for item in claim['items']:
                  report_price_of_goods += float(item['cost_value'])
          except:
              report_price_of_goods = 0
          try:
              report_goods = ""
              for item in claim['items']:
                  report_goods = report_goods + str(item['title']) + " |"
          except:
              report_goods = "---"
          try:
              report_weight_kg = 0.0
              for item in claim['items']:
                  if re.findall(r"(\d*\.?\d+)\s*(kgs?)\b", str(item['title']), flags=re.IGNORECASE):
                      report_weight_kg = report_weight_kg + float(re.findall(r"(\d*\.?\d+)\s*(kgs?)\b", str(item['title']), flags=re.IGNORECASE)[0][0])
          except:
              report_weight_kg = "---"
          try:
              report_point_A_time = datetime.datetime.strptime(claim['route_points'][0]['visited_at']['actual'],"%Y-%m-%dT%H:%M:%S.%f%z").astimezone(timezone(client_timezone))
              report_point_A_time = report_point_A_time.strftime("%Y-%m-%dT%H:%M:%S.%f%z")
          except:
              report_point_A_time = "Point A missing pick datetime"
          try:
              report_point_B_time = datetime.datetime.strptime(claim['route_points'][1]['visited_at']['actual'],"%Y-%m-%dT%H:%M:%S.%f%z").astimezone(timezone(client_timezone))
              report_point_B_time = report_point_B_time.strftime("%Y-%m-%dT%H:%M:%S.%f%z")
          except:
              report_point_B_time = "Point B was never visited"
          
          timelimit = datetime.datetime.now(timezone(client_timezone)).replace(hour=23, minute=59, second=59, microsecond=999999) - datetime.timedelta(days=period)
          if report_status_time > timelimit:
                report_status_time = report_status_time.strftime("%Y-%m-%d %H:%M")
                row = [report_client, report_client_id, report_claim_id,
                    report_pickup_address, report_receiver_address, report_comment,
                    report_status, report_status_time,report_return_reason,
                    report_longitude, report_latitude, report_status_is_final, report_point_A_time, report_point_B_time]
                report.append(row)
    result_frame = pandas.DataFrame(report,
                                    columns=["client", "client_id", "claim_id", "pickup_address", "receiver_address", 
                                             "comment", "status", "status_time", "return_reason", "lon", "lat", "is_final",
                                             "point_B_time", "point_A_time_pick"])
    return result_frame


st.markdown(f"# Informe de rutas para Juntoz")

CLAIM_SECRETS=[]
CLAIM_SECRETS.append(st.text_input("Por favor ingrese el token API",value=""))

if st.sidebar.button("Actualizar datos", type="primary"):
    st.cache_data.clear()
st.sidebar.caption(f"La recarga de la página no actualiza los datos. En su lugar, use este botón para obtener un informe nuevo")

period = st.sidebar.slider ("Seleccione la profundidad del informe en días (días desde la última actualización)", min_value=1, max_value=30, value=7)

@st.cache_data
def get_cached_report(period, CLAIM_SECRETS):
    client_timezone = "America/Lima"
    date_to = datetime.datetime.now(timezone(client_timezone)) + datetime.timedelta(days=1)
    end_ = date_to.strftime("%Y-%m-%d")
    date_from = datetime.datetime.now(timezone(client_timezone)) - datetime.timedelta(days=35)
    start_ = date_from.strftime("%Y-%m-%d")
    report = get_report(CLAIM_SECRETS, period, start_, end_)
    return report
if CLAIM_SECRETS[0] != "":
    df  = get_cached_report(period, CLAIM_SECRETS)
else:
    st.write("El token API no es válido")

statuses = st.sidebar.multiselect(
    'Filtrar por estado:',
    ['pickup_arrived',
     'pickuped',
     'delivery_arrived',
     'delivered',
     'delivered_finish',
     'returning',
     'return_arrived',
     'returned',
     'returned_finish',
     # 'cancelled_by_taxi',
     # 'cancelled',
     'performer_lookup',
     'performer_found',
     'performer_draft',
     'performer_not_found',
     'failed',
     'accepted',
     'new'])

if CLAIM_SECRETS[0] != "":
    if (not statuses or statuses == []):
        filtered_frame = df[~df["status"].isin(["estimating_failed", "cancelled", "cancelled_by_taxi", "cancelled_with_payment"])]
    else:
        filtered_frame = df[df['status'].isin(statuses)]
    filtered_frame = filtered_frame.sort_values(by=['client', 'client_id', 'status_time'], ascending=False, ignore_index=True)
    st.dataframe(filtered_frame)
    
    client_timezone = "America/Lima"
    TODAY = datetime.datetime.now(timezone(client_timezone)) - datetime.timedelta(days=1)
    
    with pandas.ExcelWriter(FILE_BUFFER, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='routes_report')
        writer.close()
    
        st.download_button(
            label="Descargar informe como xlsx",
            data=FILE_BUFFER,
            file_name=f"route_report_{TODAY}.xlsx",
            mime="application/vnd.ms-excel"
        )

st.caption("Con cariño desde YD ❤️")

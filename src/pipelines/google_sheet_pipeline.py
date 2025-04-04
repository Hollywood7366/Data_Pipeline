import gspread
import polars as pl 
from oauth2client.service_account import ServiceAccountCredentials
import time
from datetime import datetime
import threading
import os

from utils.logging import Logger

logger = Logger(name='iqfeed-symbols', log_dir='data/sheet_logs')

class GoogleSheetSync:
    def __init__(self, credentials_path, spreadsheet_key, worksheet_name=0, update_interval=60, 
                 data_folder="data", auto_save=True, filename=None):
        self.credentials_path = credentials_path
        self.spreadsheet_key = spreadsheet_key
        self.worksheet_name = worksheet_name
        self.update_interval = update_interval
        self.data_folder = data_folder
        self.auto_save = auto_save
        self.custom_filename = filename
        self.df = pl.DataFrame()
        self.last_updated = None
        self.running = False
        self.update_thread = None
        
        if not os.path.exists(self.data_folder):
            os.makedirs(self.data_folder)
            logger.debug(f"Created data folder: {self.data_folder}")
        
        self.connect()
        
        success = self.update_dataframe()
        if not success:
            logger.warning("Warning: Initial data fetch failed. Will retry during auto-update.")
    
    def connect(self):
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            self.credentials_path, scope
        )
        self.client = gspread.authorize(credentials)
        self.sheet = self.client.open_by_key(self.spreadsheet_key)
        
        if isinstance(self.worksheet_name, int):
            self.worksheet = self.sheet.get_worksheet(self.worksheet_name)
        else:
            self.worksheet = self.sheet.worksheet(self.worksheet_name)
        
        if self.custom_filename is None:
            self.filename = f"{self.worksheet.title.lower().replace(' ', '_')}.csv"
        else:
            self.filename = self.custom_filename if self.custom_filename.endswith('.csv') else f"{self.custom_filename}.csv"
    
    def update_dataframe(self):
        try:
            data = self.worksheet.get_all_values()
            
            if not data:
                logger.warning("Warning: No data found in the sheet")
                return False
                
            headers = data[0]
            duplicate_headers = [h for h in headers if headers.count(h) > 1]
            if duplicate_headers:
                logger.warning(f"Warning: Duplicate headers found: {set(duplicate_headers)}")
                unique_headers = []
                header_counts = {}
                
                for h in headers:
                    if h in header_counts:
                        header_counts[h] += 1
                        unique_headers.append(f"{h}_{header_counts[h]}")
                    else:
                        header_counts[h] = 0
                        unique_headers.append(h)
                
                headers = unique_headers
            
            values = data[1:] if len(data) > 1 else []
            new_df = pl.DataFrame(values, schema=headers)
            self.df = new_df
            self.last_updated = datetime.now()
            
            logger.info(f"DataFrame updated at {self.last_updated}, shape: {self.df.shape}")
            
            if self.auto_save:
                self.save_to_csv()
                
            return True
            
        except Exception as e:
            logger.error(f"Error updating DataFrame: {e}")
            try:
                self.connect()
            except Exception as reconnect_error:
                logger.critical(f"Reconnection failed: {reconnect_error}")
            return False
    
    def get_dataframe(self):
        if self.df is None or self.df.is_empty():
            logger.warning("Warning: DataFrame is empty or not initialized yet")
            return pl.DataFrame()
        return self.df.clone() 
    
    def get_raw_data(self):
        try:
            return self.worksheet.get_all_values()
        except Exception as e:
            logger.error(f"Error getting raw data: {e}")
            return []
    
    def save_to_csv(self, custom_filename=None):
        try:
            if self.df.empty:
                logger.warning("Warning: Cannot save empty DataFrame to CSV")
                return None
            
            filename = custom_filename or self.filename
            filepath = os.path.join(self.data_folder, filename)
            
            if not os.path.exists(self.data_folder):
                os.makedirs(self.data_folder)
            
            self.df.write_csv(filepath, quote_style='non-numeric')
            
            logger.info(f"DataFrame saved to {filepath}")
            return filepath
            
        except Exception as e:
            logger.critical(f"Error saving CSV: {e}")
            return None
    
    def start_auto_update(self):
        if self.running:
            logger.debug("Auto-update is already running")
            return
        
        self.running = True
        self.update_thread = threading.Thread(target=self._auto_update_worker)
        self.update_thread.daemon = True
        self.update_thread.start()
        logger.info(f"Auto-update started with {self.update_interval} second interval")
    
    def stop_auto_update(self):
        self.running = False
        if self.update_thread:
            self.update_thread.join(timeout=1)
        logger.debug("Auto-update stopped")
    
    def _auto_update_worker(self):
        while self.running:
            time.sleep(self.update_interval)
            if self.running:
                self.update_dataframe()


# if __name__ == "__main__":
#     CREDENTIALS_FILE = "api.json"
#     SPREADSHEET_KEY = "143mImeD54tx0q-yXBmmyXkN-hI8JwpA_0-IJSs8_Lrk"
    
#     sheet_sync = GoogleSheetSync(
#         credentials_path=CREDENTIALS_FILE,
#         spreadsheet_key=SPREADSHEET_KEY,
#         worksheet_name=0,
#         update_interval=30,
#         data_folder="data",
#         auto_save=True  
#     )

#     sheet_sync.start_auto_update()
    
#     try:
#         while True:
#             current_df = sheet_sync.get_dataframe()
#             print(f"DataFrame shape: {current_df.shape}")
            
#             time.sleep(10)
    
#     except KeyboardInterrupt:
#         sheet_sync.stop_auto_update()

#     sheet_sync.upload_dataframe(df_to_upload)
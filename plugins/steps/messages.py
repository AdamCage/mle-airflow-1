from airflow.providers.telegram.hooks.telegram import TelegramHook

def send_telegram(context):
    hook = TelegramHook(token='7647343497:AAFBl8G4Nn-46fy_kTjc0j7CN9iLeHVeMX8', chat_id='-4964848866')
    dag = context['dag'].dag_id
    run_id = context['run_id']
    
    message = f'DAG {dag} с id={run_id} завершился с ошибкой!' 
    hook.send_message({
        'chat_id': '-4964848866',
        'text': message
    })
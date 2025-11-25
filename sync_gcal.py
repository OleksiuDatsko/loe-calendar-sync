import os
import json
import datetime
import yaml
from zoneinfo import ZoneInfo
from ics import Calendar
from dotenv import load_dotenv

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from rich.console import Console
from rich.progress import track
from rich.panel import Panel

from main import main

load_dotenv()

SCOPES = ["https://www.googleapis.com/auth/calendar"]
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "schedules")
SCHEDULE_STATE_FILE = os.path.join(OUTPUT_DIR, "schedule_state.json")
SYNC_STATE_FILE = os.path.join(OUTPUT_DIR, "sync_state.json")
CONFIG_FILE = "config.yaml"

TZ_STR = os.getenv("TIMEZONE", "UTC")
TZ = ZoneInfo(TZ_STR)

console = Console()


def load_config():
    """Завантажує налаштування з YAML файлу."""
    if not os.path.exists(CONFIG_FILE):
        console.print(f"[bold red]Помилка:[/] Файл '{CONFIG_FILE}' не знайдено!")
        console.print("[yellow]Створіть файл config.yaml із секцією 'calendars'.[/]")
        exit(1)

    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        try:
            return yaml.safe_load(f)
        except yaml.YAMLError as exc:
            console.print(f"[bold red]Помилка читання YAML:[/] {exc}")
            exit(1)


def load_json(filepath):
    if os.path.exists(filepath):
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_json(filepath, data):
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        console.print(f"[red]Не вдалося зберегти стан синхронізації: {e}[/]")


def authenticate_google():
    """Авторизація через OAuth 2.0."""
    creds = None
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not os.path.exists("credentials.json"):
                console.print(
                    "[bold red]Помилка:[/] Файл 'credentials.json' не знайдено! "
                    "Скачайте його з Google Cloud Console."
                )
                exit(1)

            flow = InstalledAppFlow.from_client_secrets_file(
                "credentials.json", SCOPES
            )
            creds = flow.run_local_server(port=0)

        with open("token.json", "w") as token:
            token.write(creds.to_json())

    return build("calendar", "v3", credentials=creds)


def get_local_events(group_id):
    ics_path = os.path.join(OUTPUT_DIR, f"group_{group_id}.ics")
    if not os.path.exists(ics_path):
        return []
    with open(ics_path, "r", encoding="utf-8") as f:
        c = Calendar(f.read())
    return [e for e in c.events if "Нема світла" in e.name]


def clear_existing_blackouts(service, calendar_id, target_date, group_id):
    """Очищення подій конкретної групи за дату."""
    start_dt = datetime.datetime.combine(target_date, datetime.time.min).replace(
        tzinfo=TZ
    )
    end_dt = start_dt + datetime.timedelta(days=1)

    try:
        events_result = (
            service.events()
            .list(
                calendarId=calendar_id,
                timeMin=start_dt.isoformat(),
                timeMax=end_dt.isoformat(),
                singleEvents=True,
                orderBy="startTime",
                q="Нема світла",
            )
            .execute()
        )
    except HttpError as e:
        console.print(f"[bold red]Помилка доступу до календаря {calendar_id}:[/] {e}")
        return

    events = events_result.get("items", [])
    if not events:
        return

    events_to_delete = [
        e for e in events if f"(Гр. {group_id})" in e.get("summary", "")
    ]

    for event in track(
        events_to_delete, description=f"[yellow]Очищення старого (Гр. {group_id})...[/]"
    ):
        try:
            service.events().delete(
                calendarId=calendar_id, eventId=event["id"]
            ).execute()
        except HttpError:
            pass


def process_group(service, group_id, calendar_id, schedule_state, sync_state):
    console.print(
        f"\n[bold cyan]--- Обробка: Група {group_id} -> Календар ...{calendar_id[-5:]} ---[/]"
    )

    current_date_str = schedule_state.get("date")
    current_signature = schedule_state.get("groups", {}).get(group_id, "")

    last_synced_data = sync_state.get(group_id, {})

    if (
        current_date_str == last_synced_data.get("date")
        and current_signature == last_synced_data.get("signature")
        and calendar_id == last_synced_data.get("calendar_id")
    ):
        console.print(f"[green]✓ Графік актуальний.[/]")
        return False

    local_events = get_local_events(group_id)
    target_date = datetime.date.fromisoformat(current_date_str)

    clear_existing_blackouts(service, calendar_id, target_date, group_id)

    if local_events:
        for event in track(local_events, description=f"[green]Запис подій...[/]"):
            event_body = {
                "summary": f"{event.name} (Гр. {group_id})",
                "description": event.description,
                "start": {
                    "dateTime": event.begin.datetime.isoformat(),
                    "timeZone": str(TZ),
                },
                "end": {
                    "dateTime": event.end.datetime.isoformat(),
                    "timeZone": str(TZ),
                },
                "reminders": {
                    "useDefault": False,
                    "overrides": [{"method": "popup", "minutes": 15}],
                },
                "colorId": "11",
            }
            try:
                service.events().insert(
                    calendarId=calendar_id, body=event_body
                ).execute()
            except HttpError as error:
                console.print(f"[red]Помилка API: {error}[/]")
    else:
        console.print(f"[dim]Світло є, календар очищено.[/dim]")

    if group_id not in sync_state:
        sync_state[group_id] = {}
    sync_state[group_id]["date"] = current_date_str
    sync_state[group_id]["signature"] = current_signature
    sync_state[group_id]["calendar_id"] = calendar_id

    return True


def sync_all():
    config = load_config()

    global TZ
    if "timezone" in config:
        TZ = ZoneInfo(config["timezone"])

    calendar_mapping = config.get("calendars", {})

    if not calendar_mapping:
        console.print(
            "[bold red]Увага:[/] У config.yaml не знайдено секції 'calendars' або вона порожня."
        )
        return

    main()

    schedule_state = load_json(SCHEDULE_STATE_FILE)
    if not schedule_state.get("date"):
        console.print("[bold red]Помилка:[/] Немає даних про графік.")
        return

    sync_state = load_json(SYNC_STATE_FILE)

    service = authenticate_google()

    console.print(
        Panel(
            f"Синхронізація {len(calendar_mapping)} груп з config.yaml",
            title="Google Sync",
        )
    )

    any_changes = False
    for group_id, calendar_id in calendar_mapping.items():
        group_id = str(group_id)

        if group_id not in schedule_state.get("groups", {}):
            console.print(
                f"[red]Увага:[/] Групи '{group_id}' немає у завантажених графіках. Перевірте config.yaml."
            )
            continue

        if process_group(service, group_id, calendar_id, schedule_state, sync_state):
            any_changes = True

    if any_changes:
        save_json(SYNC_STATE_FILE, sync_state)
        console.print("\n[bold green]✨ Всі календарі оновлено![/]")
    else:
        console.print("\n[bold green]✨ Оновлень не потрібно.[/]")


if __name__ == "__main__":
    sync_all()

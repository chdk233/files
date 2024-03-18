from pcd_iot_dbx.dqi.smarthome import notion
from pcd_iot_dbx.dqi.smarthome import leakbot

NOTION_NAME_TO_DQI_CHECK = \
{"shipped_installed_daily": notion.DailyShippedInstalled,
 "sensor_monthly": notion.MonthlySensor,
 "events_weekly" : notion.WeeklyEvents,
 "system_health_daily": notion.DailySystemHealth,
 "systems_live_weekly" : notion.WeeklySystemsLive
}

LEAKBOT_NAME_TO_DQI_CHECK = \
{"events" : leakbot.Events
}

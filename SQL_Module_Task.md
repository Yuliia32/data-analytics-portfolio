# 📊 SQL Modul Task — Аналіз акаунтів і email-активності для e-commerce

👤 **Автор**: Юлія Смульченко  
📅 **Дата**: 05.04.2025

---

## 🎯 Мета

- Проаналізувати динаміку створення акаунтів
- Активність користувачів у розрізі листів (відправлення, відкриття, переходи)
- Поведінку за параметрами: інтервал відправки, верифікація, підписка
- Виявити топ-10 країн за рівнем активності

---

## 🧩 SQL-Запит

```sql
WITH

-- 1. Метрики по email-повідомленнях
message_data AS (
  SELECT
    s.date,
    sp.country,
    a.send_interval,
    a.is_verified,
    a.is_unsubscribed,
    COUNT(DISTINCT eo.id_message) AS sent_msg,
    COUNT(DISTINCT es.id_message) AS open_msg,
    COUNT(DISTINCT ev.id_message) AS click_msg
  FROM DA.email_sent eo
  LEFT JOIN DA.email_open es ON es.id_message = eo.id_message
  LEFT JOIN DA.email_visit ev ON ev.id_message = eo.id_message
  JOIN DA.account a ON a.id = eo.id_account
  JOIN DA.session s ON a.id = s.ga_session_id
  JOIN DA.session_params sp ON s.ga_session_id = sp.ga_session_id
  WHERE s.date IS NOT NULL
  GROUP BY s.date, sp.country, a.send_interval, a.is_verified, a.is_unsubscribed
),

-- 2. Метрики по акаунтах
account_data AS (
  SELECT
    s.date,
    sp.country,
    a.send_interval,
    a.is_verified,
    a.is_unsubscribed,
    COUNT(DISTINCT a.id) AS account_cnt
  FROM DA.account a
  JOIN DA.session s ON a.id = s.ga_session_id
  JOIN DA.session_params sp ON s.ga_session_id = sp.ga_session_id
  WHERE s.date IS NOT NULL
  GROUP BY s.date, sp.country, a.send_interval, a.is_verified, a.is_unsubscribed
),

-- 3. Об'єднання даних
combined_data AS (
  SELECT
    date, country, send_interval, is_verified, is_unsubscribed,
    sent_msg, open_msg, click_msg, NULL AS account_cnt
  FROM message_data
  UNION ALL
  SELECT
    date, country, send_interval, is_verified, is_unsubscribed,
    NULL AS sent_msg, NULL AS open_msg, NULL AS click_msg, account_cnt
  FROM account_data
),

-- 4. Агрегація метрик
email_metrics AS (
  SELECT
    date,
    country,
    send_interval,
    is_verified,
    is_unsubscribed,
    SUM(sent_msg) AS sent_msg,
    SUM(open_msg) AS open_msg,
    SUM(click_msg) AS click_msg,
    SUM(account_cnt) AS account_cnt
  FROM combined_data
  GROUP BY date, country, send_interval, is_verified, is_unsubscribed
),

-- 5. Тотали та обчислення частки email/account
total_metrics AS (
  SELECT *,
    SUM(sent_msg) OVER (PARTITION BY country) AS total_country_sent_cnt,
    SUM(account_cnt) OVER (PARTITION BY country) AS total_country_account_cnt,
    SAFE_DIVIDE(SUM(sent_msg) OVER (PARTITION BY country), SUM(account_cnt) OVER (PARTITION BY country)) AS email_per_account
  FROM email_metrics
),

-- 6. Ранжування країн
with_enriched_data AS (
  SELECT *,
    DENSE_RANK() OVER (ORDER BY total_country_sent_cnt DESC) AS rank_total_country_sent,
    DENSE_RANK() OVER (ORDER BY total_country_account_cnt DESC) AS rank_total_country_account
  FROM total_metrics
)

-- 7. Фінальний вибір
SELECT *
FROM with_enriched_data
WHERE rank_total_country_sent <= 10
  AND rank_total_country_account <= 10;

 Коментарі до логіки запиту
message_data — обчислення email-метрик (надіслані, відкриті, клікнуті листи).

account_data — обчислення кількості акаунтів за параметрами.

combined_data — об'єднання листів та акаунтів для подальшої агрегації.

email_metrics — сумування метрик на рівні дати + країни.

total_metrics — обчислення загальної кількості листів і акаунтів по країнах, розрахунок email_per_account.

with_enriched_data — ранжування країн.

Фінальний SELECT — вибір топ-10 країн за обома метриками.

 Ключові інсайти
США має найбільшу кількість акаунтів (4 852) і найвищу активність.

Індія та Канада — друге і третє місця відповідно.

Піки email-розсилок — середина жовтня, початок листопада, початок грудня.

Найвищий open rate при інтервалі розсилок в 3 дні (~57%).

98–99% відкриттів і кліків роблять верифіковані акаунти.

 Рекомендації
Зосередити маркетингові активності на США.

Оптимальний інтервал розсилок — раз на 3 дні.

Основні email-кампанії запускати на початку місяця.

Фокус на верифікованих користувачах і стимулювання нових до верифікації.

 Використані інструменти
SQL (BigQuery)

Аналітичні функції: SUM() OVER, DENSE_RANK() OVER, SAFE_DIVIDE()

Створення агрегованих дата-сетів для візуалізації в дашбордах


import re
from datetime import datetime, timedelta
from typing import Any

import requests
from bs4 import BeautifulSoup


def get_resume_urls_from_page(url: str) -> list[str]:
    response = requests.get(url)
    if response.status_code != 200:
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    result = []

    divs = soup.find_all("div", class_="f-test-search-result-item")

    for div in divs:
        anchor = div.find("a", class_="EruXX")
        if anchor is not None:
            if anchor.get("href").startswith("/resume"):
                result.append(rf"https://www.superjob.ru/{anchor.get('href')}")

    return result


def str_date_to_datetime(date: str) -> datetime:
    if "вчера" in date:
        return datetime.now() - timedelta(days=1)

    months_dict = {
        "января": 1,
        "февраля": 2,
        "марта": 3,
        "апреля": 4,
        "мая": 5,
        "июня": 6,
        "июля": 7,
        "августа": 8,
        "сентября": 9,
        "октября": 10,
        "ноября": 11,
        "декабря": 12,
    }

    month = 0
    for month_str, number in months_dict.items():
        if month_str in date:
            month = number

    if not month:
        return datetime.now()

    try:
        day = re.findall(r"\d{2} ", date)[0].strip()
        year_found = re.findall(r"20\d{2}", date)
        if year_found:
            year = year_found[0]
        else:
            year = 2024

        return datetime(year=year, month=month, day=day)
    except Exception:
        return datetime(year=1970, month=1, day=1)


def get_date_of_updating(soup: BeautifulSoup) -> datetime:

    div = soup.find("div", class_="e1UIb")

    updating_dates = div.find_all("span", class_="lkr9c Qpqo3 _31H4p cq8in")

    updating_date: str = updating_dates[1].text.strip() if updating_dates[1] is not None else ""
    return str_date_to_datetime(updating_date)


def get_age(soup: BeautifulSoup) -> str:
    age = soup.find("span", class_="DzbIT s24Iy _1yskz _3Bzp6 lkr9c Qpqo3 _1vBD3 cq8in")
    result_age: str = age.text.strip().replace("\xa0", " ") if age is not None else "-1"
    return result_age


def get_salary(soup: BeautifulSoup) -> str:
    salary = soup.find("span", class_="-Hv1l Qpqo3 B7FnQ")
    result_salary: str = salary.text.strip().replace("\xa0", " ")
    return result_salary


def get_desired_position(soup: BeautifulSoup) -> str:
    desired_position = soup.find("h1", class_="VB8-V -Hv1l Qpqo3 _2m2xE")
    result_desired_position: str = desired_position.text.strip() if desired_position else ""
    return result_desired_position


def get_city(soup: BeautifulSoup) -> str:
    city = soup.find("div", class_="J+R2u")

    city = city.get_text()
    result_city: str = city.split(",")[0].strip() if city else ""

    return result_city


def get_working_conditions(soup: BeautifulSoup) -> str:
    div = soup.find("div", class_="Xkibi")

    working_conditions = div.find("div", class_="J+R2u")

    working_conditions = working_conditions.get_text()
    working_conditions = working_conditions.split(",")[1:]
    result_working_conditions: str = ",".join(working_conditions).strip().replace("\xa0", " ")

    return result_working_conditions


def get_skills(soup: BeautifulSoup) -> str:
    skills = soup.find("ul", class_="_8jaXR _1nNwC _2P41q bn_Xt _1kYH3")
    if skills is None:
        return ""

    skills = skills.find_all("li", class_="_19Wau")
    skills = [tag.get_text().strip() for tag in skills if tag.get_text().strip() != "Показать еще"]
    result_skills: str = ", ".join(skills)

    return result_skills


def get_employment(soup: BeautifulSoup) -> str:
    div = soup.find("div", class_="vK4Mq _2NPzg _1-86a _3umqY _2w28p Kwuox")

    employment = div.find("span", class_="lkr9c Qpqo3 _1vBD3 B7FnQ")
    result_employment: str = employment.text.strip().replace("\xa0", " ") if employment else ""

    return result_employment


def get_last_experience(soup: BeautifulSoup) -> tuple[str, str]:
    div = soup.find("div", class_="Ed+Mf")

    if not div:
        return "", ""

    last_current_place_of_work = div.find("span", class_="lkr9c Qpqo3 _31H4p B7FnQ _3YZZG")
    last_current_place_of_work = last_current_place_of_work.get_text().strip() if last_current_place_of_work else ""

    last_current_position = div.find("h3", class_="_1g0P1 Qpqo3 _1vBD3 B7FnQ _3YZZG")
    last_current_position = last_current_position.get_text().strip() if last_current_position else ""

    return last_current_place_of_work, last_current_position


def get_education(soup: BeautifulSoup) -> str:
    div = soup.find("div", class_="f-test-block-account_balance")

    if not div:
        return ""

    uni_name = div.find("h3", class_="_1g0P1 Qpqo3 _1vBD3 B7FnQ _3YZZG")
    result_uni_name: str = uni_name.get_text().strip() if uni_name else ""

    return result_uni_name


def get_data_from_resume_by_url(url: str) -> dict[str, Any]:
    info: dict[str, Any] = {}

    response = requests.get(url)
    if response.status_code != 200:
        print(f"Не удалось загрузить страницу: {response.status_code}")
        return {}

    soup = BeautifulSoup(response.text, "html.parser")

    info["Дата обновления резюме"] = get_date_of_updating(soup)
    info["Возраст"] = get_age(soup)
    info["ЗП"] = get_salary(soup)
    info["Желаемая должность"] = get_desired_position(soup)
    info["Город"] = get_city(soup)
    info["Условия работы"] = get_working_conditions(soup)
    info["Занятость"] = get_employment(soup)
    info["Навыки"] = get_skills(soup)

    experience = get_last_experience(soup)
    info["Последнее/текущее место работы"] = experience[0]
    info["Последняя/текущая должность"] = experience[1]

    info["Образование и ВУЗ"] = get_education(soup)

    return info

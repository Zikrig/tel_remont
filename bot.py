import asyncio
import json
import os
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
from dotenv import load_dotenv

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.json")


def load_config() -> Dict[str, Any]:
    if not os.path.exists(CONFIG_PATH):
        raise FileNotFoundError(f"Config file not found: {CONFIG_PATH}")
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_config(cfg: Dict[str, Any]) -> None:
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)


def get_cfg() -> Dict[str, Any]:
    global config
    config = load_config()
    return config


config = load_config()

REPAIR_PROBLEM_KEYS = [
    ("screen", "Экран"),
    ("battery", "Аккумулятор"),
    ("no_power", "Не включается"),
    ("cleaning", "Чистка"),
    ("upgrade", "Увеличение"),
    ("keyboard", "Клавиатура"),
    ("firmware", "Прошивка"),
    ("water_damage", "После влаги"),
    ("other", "Другое"),
]

ADMIN_EDITABLE_FIELDS = {
    "welcome_text": "Приветствие",
    "menu_text": "Текст перед выбором услуги",
    "hydro_text": "Текст «Гидрогелевая плёнка»",
    "price_text": "Текст «Стоимость защиты и аксессуаров»",
    "warranty_text": "Текст «Хочу обратиться по гарантии»",
    "device_in_service_prompt": "Подсказка «Моё устройство в сервисе»",
    "device_in_service_phone_question": "Вопрос про доступность по номеру",
    "ask_contact_text": "Запрос контакта (общий)",
    "offices": "Офисы (JSON-массив строк)",
    "office_addresses": "Адреса офисов (JSON-массив строк)",
    "repair_categories": "Категории ремонта (JSON-массив объектов key/title)",
    "repair_models_by_category": "Модели по категориям (JSON-объект key -> массив)",
    "repair_laptop_models": "Модели ноутбуков (JSON-массив строк)",
    "repair_problem_texts": "Описания/цены проблем (JSON-объект)",
    "repair_time_slots": "Слоты времени (JSON-массив строк)",
    "repair_confirmation_template": "Шаблон подтверждения записи",
}

ADMIN_JSON_FIELDS = {
    "offices",
    "office_addresses",
    "repair_categories",
    "repair_models_by_category",
    "repair_laptop_models",
    "repair_problem_texts",
    "repair_time_slots",
}


class UserStates(StatesGroup):
    waiting_device_office = State()
    waiting_device_info = State()
    waiting_contact_for_device = State()
    repair_choose_category = State()
    repair_choose_model = State()
    repair_choose_problem = State()
    repair_other_problem = State()
    repair_description = State()
    repair_choose_office = State()
    repair_choose_day = State()
    repair_enter_date = State()
    repair_choose_time = State()
    repair_ask_name = State()
    repair_ask_contact = State()


class AdminEditStates(StatesGroup):
    waiting_new_value = State()


def get_admin_ids() -> List[int]:
    raw = os.getenv("ADMIN_IDS", "").strip()
    if not raw:
        raise RuntimeError("ADMIN_IDS is not set in environment")
    result: List[int] = []
    for part in raw.split(","):
        value = part.strip()
        if not value:
            continue
        try:
            result.append(int(value))
        except ValueError:
            print(f"[WARN] ADMIN_IDS contains invalid value: {value}")
    if not result:
        raise RuntimeError("ADMIN_IDS does not contain valid IDs")
    return result


def is_admin(user_id: int) -> bool:
    return user_id in get_admin_ids()


def chunks(lst: List[Any], n: int) -> List[List[Any]]:
    return [lst[i : i + n] for i in range(0, len(lst), n)]


def normalize_text(value: str) -> str:
    # Allow admins to enter \n in config and get real line breaks in messages.
    return value.replace("\\n", "\n")


def cfg_text(key: str, default: str) -> str:
    get_cfg()
    value = config.get(key, default)
    if not isinstance(value, str):
        return default
    return normalize_text(value)


def main_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Ремонт / диагностика", callback_data="menu_repair")],
            [InlineKeyboardButton(text="Гидрогелевая плёнка", callback_data="menu_hydro")],
            [InlineKeyboardButton(text="Стоимость защиты и аксессуаров", callback_data="menu_price")],
            [InlineKeyboardButton(text="Моё устройство в сервисе", callback_data="menu_device_service")],
            [InlineKeyboardButton(text="Хочу обратиться по гарантии", callback_data="menu_warranty")],
        ]
    )


def yes_no_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Да", callback_data="yes_available"),
                InlineKeyboardButton(text="Нет", callback_data="no_available"),
            ]
        ]
    )


def admin_menu_keyboard() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text=label, callback_data=f"admin_edit_{field}")]
        for field, label in ADMIN_EDITABLE_FIELDS.items()
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def repair_categories_keyboard() -> InlineKeyboardMarkup:
    get_cfg()
    categories = config.get("repair_categories", [])
    if not categories:
        categories = [{"key": "phones", "title": "Смартфоны"}, {"key": "laptops", "title": "Ноутбуки"}]
    rows: List[List[InlineKeyboardButton]] = []
    for block in chunks(categories, 3):
        rows.append(
            [
                InlineKeyboardButton(text=item["title"], callback_data=f"repair_cat_{item['key']}")
                for item in block
            ]
        )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def repair_models_keyboard(models: List[str]) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    indexed_models = list(enumerate(models))
    for block in chunks(indexed_models, 3):
        rows.append(
            [InlineKeyboardButton(text=name, callback_data=f"repair_model_{idx}") for idx, name in block]
        )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def repair_problems_keyboard() -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for block in chunks(REPAIR_PROBLEM_KEYS, 3):
        rows.append(
            [InlineKeyboardButton(text=title, callback_data=f"repair_prob_{key}") for key, title in block]
        )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def offices_keyboard(prefix: str) -> InlineKeyboardMarkup:
    get_cfg()
    rows = [
        [InlineKeyboardButton(text=office, callback_data=f"{prefix}_{idx}")]
        for idx, office in enumerate(config.get("offices", []))
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def repair_day_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Завтра", callback_data="repair_day_tomorrow")],
            [InlineKeyboardButton(text="Другой день", callback_data="repair_day_other")],
        ]
    )


def repair_time_keyboard() -> InlineKeyboardMarkup:
    get_cfg()
    slots = config.get("repair_time_slots", [])
    any_time = config.get("repair_any_time_text", "Приду в течение дня")
    rows: List[List[InlineKeyboardButton]] = []
    for block in chunks(list(enumerate(slots)), 3):
        rows.append([InlineKeyboardButton(text=slot, callback_data=f"repair_time_{idx}") for idx, slot in block])
    rows.append([InlineKeyboardButton(text=any_time, callback_data="repair_time_any")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def send_to_admin(bot: Bot, text: str) -> bool:
    delivered = False
    for admin_id in get_admin_ids():
        try:
            await bot.send_message(chat_id=admin_id, text=text)
            delivered = True
        except TelegramBadRequest as e:
            print(f"[WARN] Failed to send to admin {admin_id}: {e}")
    return delivered


def is_phone_number(value: str) -> bool:
    return value.isdigit() and len(value) == 11 and value.startswith("7")


def is_order_number(value: str) -> bool:
    return bool(re.fullmatch(r"\d{3}-\d{5}", value))


def parse_date_ru(value: str) -> Optional[datetime]:
    try:
        return datetime.strptime(value, "%d.%m.%Y")
    except ValueError:
        return None


def get_models_for_category(category_key: str) -> List[str]:
    get_cfg()
    if category_key == "laptops":
        return config.get("repair_laptop_models", [])
    models_by_category = config.get("repair_models_by_category", {})
    return models_by_category.get(category_key, [])


def category_title_by_key(category_key: str) -> str:
    get_cfg()
    for item in config.get("repair_categories", []):
        if item.get("key") == category_key:
            return item.get("title", category_key)
    return category_key


def office_address_by_idx(idx: int) -> str:
    get_cfg()
    addresses = config.get("office_addresses", [])
    if 0 <= idx < len(addresses):
        return addresses[idx]
    offices = config.get("offices", [])
    return offices[idx] if 0 <= idx < len(offices) else "Адрес уточнит администратор"


async def start_handler(message: Message, state: FSMContext) -> None:
    await state.clear()
    await message.answer(cfg_text("welcome_text", "Здравствуйте!"), reply_markup=main_menu_keyboard())


async def menu_repair_handler(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    await state.set_state(UserStates.repair_choose_category)
    await callback.message.edit_text(
        "Выберите категорию устройства:",
        reply_markup=repair_categories_keyboard(),
    )


async def repair_category_handler(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    key = callback.data.replace("repair_cat_", "", 1)
    title = category_title_by_key(key)
    await state.update_data(repair_category_key=key, repair_category_title=title)

    models = get_models_for_category(key)
    if models:
        await state.update_data(repair_models=models)
        await state.set_state(UserStates.repair_choose_model)
        await callback.message.edit_text(
            "Выберите модель:",
            reply_markup=repair_models_keyboard(models),
        )
        return

    await state.update_data(repair_device=title)
    await state.set_state(UserStates.repair_choose_problem)
    await callback.message.edit_text(
        "Выберите проблему:",
        reply_markup=repair_problems_keyboard(),
    )


async def repair_model_handler(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    idx_raw = callback.data.replace("repair_model_", "", 1)
    if not idx_raw.isdigit():
        return
    idx = int(idx_raw)
    data = await state.get_data()
    models = data.get("repair_models", [])
    if idx < 0 or idx >= len(models):
        return

    await state.update_data(repair_device=models[idx])
    await state.set_state(UserStates.repair_choose_problem)
    await callback.message.edit_text(
        "Выберите проблему:",
        reply_markup=repair_problems_keyboard(),
    )


async def repair_problem_handler(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    problem_key = callback.data.replace("repair_prob_", "", 1)
    title = next((t for k, t in REPAIR_PROBLEM_KEYS if k == problem_key), problem_key)
    await state.update_data(repair_problem_key=problem_key, repair_problem_title=title)
    get_cfg()
    text = config.get("repair_problem_texts", {}).get(problem_key, f"{title}\n\nЦену уточним после диагностики.")
    if isinstance(text, str):
        text = normalize_text(text)

    if problem_key == "other":
        await state.set_state(UserStates.repair_other_problem)
        await callback.message.edit_text(
            f"{text}\n\n{cfg_text('repair_other_problem_prompt', 'Опишите проблему.')}"
        )
        return

    await state.set_state(UserStates.repair_description)
    await callback.message.edit_text(
        f"{text}\n\n{cfg_text('repair_description_prompt', 'Опишите проблему подробнее.')}"
    )


async def repair_other_problem_handler(message: Message, state: FSMContext) -> None:
    await state.update_data(repair_problem_custom=(message.text or "").strip())
    await state.set_state(UserStates.repair_description)
    await message.answer(cfg_text("repair_description_prompt", "Опишите проблему подробнее."))


async def repair_description_handler(message: Message, state: FSMContext) -> None:
    await state.update_data(repair_description=(message.text or "").strip())
    await state.set_state(UserStates.repair_choose_office)
    await message.answer(
        cfg_text("repair_choose_office_text", "Выберите офис:"),
        reply_markup=offices_keyboard("repair_office"),
    )


async def repair_office_handler(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    idx_raw = callback.data.replace("repair_office_", "", 1)
    if not idx_raw.isdigit():
        return
    idx = int(idx_raw)
    offices = config.get("offices", [])
    if idx < 0 or idx >= len(offices):
        return
    await state.update_data(repair_office_idx=idx, repair_office_name=offices[idx])
    await state.set_state(UserStates.repair_choose_day)
    await callback.message.edit_text(
        cfg_text("repair_choose_day_text", "Выберите день записи:"),
        reply_markup=repair_day_keyboard(),
    )


async def repair_day_handler(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    if callback.data == "repair_day_tomorrow":
        date_str = (datetime.now() + timedelta(days=1)).strftime("%d.%m.%Y")
        await state.update_data(repair_date=date_str)
        await state.set_state(UserStates.repair_choose_time)
        await callback.message.edit_text(
            cfg_text("repair_choose_time_text", "Выберите время:"),
            reply_markup=repair_time_keyboard(),
        )
        return
    await state.set_state(UserStates.repair_enter_date)
    await callback.message.edit_text(
        cfg_text("repair_enter_date_text", "Введите дату ДД.ММ.ГГГГ")
    )


async def repair_date_input_handler(message: Message, state: FSMContext) -> None:
    date_raw = (message.text or "").strip()
    dt = parse_date_ru(date_raw)
    if dt is None:
        await message.answer("Неверный формат даты. Используйте ДД.ММ.ГГГГ.")
        return
    await state.update_data(repair_date=date_raw)
    await state.set_state(UserStates.repair_choose_time)
    await message.answer(
        cfg_text("repair_choose_time_text", "Выберите время:"),
        reply_markup=repair_time_keyboard(),
    )


async def repair_time_handler(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    slots = config.get("repair_time_slots", [])
    any_time = cfg_text("repair_any_time_text", "Приду в течение дня")
    if callback.data == "repair_time_any":
        picked = any_time
    else:
        idx_raw = callback.data.replace("repair_time_", "", 1)
        if not idx_raw.isdigit():
            return
        idx = int(idx_raw)
        if idx < 0 or idx >= len(slots):
            return
        picked = slots[idx]
    await state.update_data(repair_time=picked)
    await state.set_state(UserStates.repair_ask_name)
    await callback.message.edit_text(cfg_text("repair_ask_name_text", "Как вас зовут?"))


async def repair_name_handler(message: Message, state: FSMContext) -> None:
    await state.update_data(repair_name=(message.text or "").strip())
    await state.set_state(UserStates.repair_ask_contact)
    await message.answer(cfg_text("repair_ask_contact_text", "Укажите телефон или @username"))


async def repair_contact_handler(message: Message, state: FSMContext) -> None:
    get_cfg()
    contact = (message.text or "").strip()
    data = await state.get_data()
    date_val = data.get("repair_date", "дата не указана")
    time_val = data.get("repair_time", "время не указано")
    device_val = data.get("repair_device", data.get("repair_category_title", "устройство"))
    problem_val = data.get("repair_problem_title", "ремонт")
    if data.get("repair_problem_key") == "other" and data.get("repair_problem_custom"):
        problem_val = f"другое: {data.get('repair_problem_custom')}"
    office_idx = data.get("repair_office_idx", 0)
    office_name = data.get("repair_office_name", "Офис")
    office_address = office_address_by_idx(office_idx)

    confirmation_template = cfg_text(
        "repair_confirmation_template",
        "Ок, записал вас на {date} в {time} на {problem} для устройства {device}.\n\nЖдём вас по адресу: {office}",
    )
    user_text = confirmation_template.format(
        date=date_val,
        time=time_val,
        problem=problem_val,
        device=device_val,
        office=office_address,
    )

    admin_text = (
        f"{cfg_text('repair_send_to_admin_header', '🛠 Новая запись на ремонт / диагностику')}\n\n"
        f"Категория: {data.get('repair_category_title', '-')}\n"
        f"Модель/устройство: {device_val}\n"
        f"Проблема: {problem_val}\n"
        f"Описание: {data.get('repair_description', '-')}\n\n"
        f"Офис: {office_name}\n"
        f"Адрес: {office_address}\n"
        f"Дата: {date_val}\n"
        f"Слот: {time_val}\n\n"
        f"Клиент: {data.get('repair_name', '-')}\n"
        f"Контакт: {contact}\n"
        f"TG user: {message.from_user.full_name} (@{message.from_user.username})\n"
        f"ID: {message.from_user.id}"
    )
    delivered = await send_to_admin(message.bot, admin_text)
    await state.clear()
    await message.answer(
        f"{user_text}\n\n"
        + (
            "Контакт для связи получили. Главное меню:"
            if delivered
            else "Контакт получили, но не смогли отправить заявку администратору. Проверьте список ADMIN_IDS."
        ),
        reply_markup=main_menu_keyboard(),
    )


async def menu_hydro_handler(callback: CallbackQuery) -> None:
    await callback.answer()
    await callback.message.edit_text(
        cfg_text("hydro_text", "Информация о гидрогелевой плёнке."),
        reply_markup=main_menu_keyboard(),
    )


async def menu_price_handler(callback: CallbackQuery) -> None:
    await callback.answer()
    await callback.message.edit_text(
        cfg_text("price_text", "Информация о стоимости защиты и аксессуаров."),
        reply_markup=main_menu_keyboard(),
    )


async def menu_warranty_handler(callback: CallbackQuery) -> None:
    await callback.answer()
    await callback.message.edit_text(
        cfg_text("warranty_text", "Информация по гарантии."),
        reply_markup=main_menu_keyboard(),
    )


async def menu_device_service_handler(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    await state.set_state(UserStates.waiting_device_office)
    await callback.message.edit_text(
        "Выберите офис, в который обращались:",
        reply_markup=offices_keyboard("device_office"),
    )


async def device_office_handler(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    idx_raw = callback.data.replace("device_office_", "", 1)
    if not idx_raw.isdigit():
        return
    idx = int(idx_raw)
    offices = config.get("offices", [])
    office_name = offices[idx] if 0 <= idx < len(offices) else "Неизвестный офис"
    await state.update_data(chosen_office=office_name)
    await state.set_state(UserStates.waiting_device_info)
    await callback.message.edit_text(
        cfg_text("device_in_service_prompt", "Напишите номер заказа или телефон, указанный в заказе.")
    )


async def handle_device_info(message: Message, state: FSMContext) -> None:
    value = (message.text or "").strip()
    await state.update_data(device_raw_input=value)
    if is_phone_number(value):
        await state.update_data(device_phone=value)
        await message.answer(
            cfg_text("device_in_service_phone_question", "Вы сейчас доступны по этому номеру телефона?"),
            reply_markup=yes_no_keyboard(),
        )
        return
    await state.update_data(device_order=value if is_order_number(value) else None)
    await state.set_state(UserStates.waiting_contact_for_device)
    await message.answer(cfg_text("ask_contact_text", "Пожалуйста, отправьте телефон или @username"))


async def handle_device_contact(message: Message, state: FSMContext) -> None:
    contact = (message.text or "").strip()
    data = await state.get_data()
    admin_text = (
        "📦 Запрос по устройству в сервисе\n\n"
        f"Офис: {data.get('chosen_office', 'Не выбран')}\n"
        f"Пользователь: {message.from_user.full_name} (@{message.from_user.username})\n"
        f"ID: {message.from_user.id}\n\n"
        f"Исходные данные пользователя: {data.get('device_raw_input', '')}\n"
    )
    if data.get("device_order"):
        admin_text += f"Распознанный номер заказа: {data.get('device_order')}\n"
    if data.get("device_phone"):
        admin_text += f"Распознанный номер телефона из заказа: {data.get('device_phone')}\n"
    admin_text += f"Контакт для связи: {contact}"
    delivered = await send_to_admin(message.bot, admin_text)
    await state.clear()
    await message.answer(
        (
            "Спасибо! Контакт получили, передали запрос администратору. Скоро свяжемся с вами."
            if delivered
            else "Контакт получили, но не удалось передать запрос администратору. "
            "Пожалуйста, проверьте корректность ADMIN_IDS."
        ),
        reply_markup=main_menu_keyboard(),
    )


async def handle_yes_no_available(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    data = await state.get_data()
    if callback.data == "yes_available":
        admin_text = (
            "📦 Запрос по устройству в сервисе\n\n"
            f"Офис: {data.get('chosen_office', 'Не выбран')}\n"
            f"Пользователь: {callback.from_user.full_name} (@{callback.from_user.username})\n"
            f"ID: {callback.from_user.id}\n\n"
            f"Исходные данные пользователя: {data.get('device_raw_input', '')}\n"
            f"Телефон для связи (доступен): {data.get('device_phone')}"
        )
        delivered = await send_to_admin(callback.bot, admin_text)
        await state.clear()
        await callback.message.edit_reply_markup(reply_markup=None)
        await callback.message.answer(
            (
                "Спасибо! Мы получили запрос и свяжемся с вами по указанному номеру."
                if delivered
                else "Номер получили, но не удалось передать запрос администратору. "
                "Проверьте корректность ADMIN_IDS."
            ),
            reply_markup=main_menu_keyboard(),
        )
        return
    await state.set_state(UserStates.waiting_contact_for_device)
    await callback.message.edit_text(cfg_text("ask_contact_text", "Пожалуйста, отправьте телефон или @username"))


async def admin_command_handler(message: Message) -> None:
    if not is_admin(message.from_user.id):
        return
    await message.answer(
        "Админ-меню. Выберите параметр для редактирования.",
        reply_markup=admin_menu_keyboard(),
    )


async def admin_edit_button_handler(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    await callback.answer()
    field_key = callback.data.replace("admin_edit_", "", 1)
    if field_key not in ADMIN_EDITABLE_FIELDS:
        await callback.message.answer("Неизвестный параметр.")
        return

    get_cfg()
    await state.set_state(AdminEditStates.waiting_new_value)
    await state.update_data(edit_field=field_key)
    current_value = config.get(field_key)
    if field_key in ADMIN_JSON_FIELDS:
        serialized = json.dumps(current_value, ensure_ascii=False, indent=2)
        hint = "Отправьте новый JSON одним сообщением."
    else:
        serialized = str(current_value or "")
        hint = cfg_text("texts_edit_hint", "Отправьте новый текст одним сообщением.")

    await callback.message.edit_text(
        f"Редактирование: {ADMIN_EDITABLE_FIELDS[field_key]}\n\n{hint}\n\nТекущее значение:\n{serialized}"
    )


async def admin_new_value_handler(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id):
        return

    data = await state.get_data()
    field_key = data.get("edit_field")
    if not field_key:
        await state.clear()
        await message.answer("Поле не выбрано.")
        return
    get_cfg()
    raw = message.text or ""
    if field_key in ADMIN_JSON_FIELDS:
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            await message.answer("Ошибка JSON. Проверьте формат и отправьте снова.")
            return
        config[field_key] = parsed
    else:
        config[field_key] = raw
    save_config(config)
    await state.clear()
    await message.answer("Сохранено.", reply_markup=admin_menu_keyboard())


async def main() -> None:
    load_dotenv()
    bot_token = os.getenv("BOT_TOKEN")
    if not bot_token:
        raise RuntimeError("BOT_TOKEN is not set in environment")

    bot = Bot(
        token=bot_token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher(storage=MemoryStorage())

    dp.message.register(start_handler, CommandStart())
    dp.message.register(admin_command_handler, Command("admin"))

    dp.callback_query.register(menu_repair_handler, F.data == "menu_repair")
    dp.callback_query.register(menu_hydro_handler, F.data == "menu_hydro")
    dp.callback_query.register(menu_price_handler, F.data == "menu_price")
    dp.callback_query.register(menu_device_service_handler, F.data == "menu_device_service")
    dp.callback_query.register(menu_warranty_handler, F.data == "menu_warranty")

    dp.callback_query.register(repair_category_handler, F.data.startswith("repair_cat_"))
    dp.callback_query.register(repair_model_handler, F.data.startswith("repair_model_"))
    dp.callback_query.register(repair_problem_handler, F.data.startswith("repair_prob_"))
    dp.message.register(repair_other_problem_handler, UserStates.repair_other_problem)
    dp.message.register(repair_description_handler, UserStates.repair_description)
    dp.callback_query.register(repair_office_handler, F.data.startswith("repair_office_"))
    dp.callback_query.register(repair_day_handler, F.data.in_(["repair_day_tomorrow", "repair_day_other"]))
    dp.message.register(repair_date_input_handler, UserStates.repair_enter_date)
    dp.callback_query.register(repair_time_handler, F.data.startswith("repair_time_"))
    dp.message.register(repair_name_handler, UserStates.repair_ask_name)
    dp.message.register(repair_contact_handler, UserStates.repair_ask_contact)

    dp.callback_query.register(device_office_handler, F.data.startswith("device_office_"))
    dp.message.register(handle_device_info, UserStates.waiting_device_info)
    dp.message.register(handle_device_contact, UserStates.waiting_contact_for_device)
    dp.callback_query.register(handle_yes_no_available, F.data.in_(["yes_available", "no_available"]))

    dp.callback_query.register(admin_edit_button_handler, F.data.startswith("admin_edit_"))
    dp.message.register(admin_new_value_handler, AdminEditStates.waiting_new_value)

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())


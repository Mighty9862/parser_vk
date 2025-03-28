import vk_api
import pandas as pd
import csv
import time
import os
import json
from getpass import getpass
from typing import Dict, List, Optional, Set, Tuple, Union, Any


def auth_vk(token: Optional[str] = None) -> Optional[vk_api.vk_api.VkApiMethod]:
    """
    Аутентификация в ВКонтакте.
    
    Args:
        token (Optional[str]): Токен для авторизации (если есть).
        
    Returns:
        Optional[vk_api.vk_api.VkApiMethod]: API-клиент VK или None при ошибке.
    """
    # Если передан токен, используем его
    if token:
        try:
            vk_session = vk_api.VkApi(token=token)
            vk = vk_session.get_api()
            # Проверка валидности токена
            test = vk.users.get()
            print("Авторизация по токену успешна!")
            return vk
        except Exception as e:
            print(f"Ошибка при авторизации по токену: {e}")
            print("Попробуем другие методы авторизации...")
    
    # Проверка наличия сохраненного токена
    token_file = "vk_token.txt"
    if os.path.exists(token_file):
        with open(token_file, "r") as f:
            token = f.read().strip()
            try:
                vk_session = vk_api.VkApi(token=token)
                vk = vk_session.get_api()
                # Проверка валидности токена
                test = vk.users.get()
                print("Авторизация по сохраненному токену успешна!")
                return vk
            except Exception as e:
                print(f"Ошибка при авторизации по сохраненному токену: {e}")
                print("Попробуем авторизоваться по логину и паролю...")
    
    # Авторизация через логин и пароль
    login = input("Введите логин от ВК (email или телефон): ")
    password = getpass("Введите пароль: ")
    
    # Функция для обработки двухфакторной аутентификации
    def two_factor() -> str:
        code = input("Введите код двухфакторной аутентификации: ")
        return code
    
    # Создание сессии vk_api
    try:
        vk_session = vk_api.VkApi(login, password, auth_handler=two_factor)
        vk_session.auth(token_only=True)
        print("Авторизация успешна!")
        
        # Сохранение токена в файл для последующих запусков
        with open(token_file, "w") as f:
            f.write(vk_session.token['access_token'])
        print(f"Токен сохранен в файл {token_file}")
        
        return vk_session.get_api()
    except vk_api.AuthError as error_msg:
        print(f"Ошибка авторизации: {error_msg}")
        
        # Предлагаем ввести токен вручную
        manual_token = input("Хотите ввести токен ВК вручную? (да/нет): ")
        if manual_token.lower() in ["да", "д", "yes", "y"]:
            token = input("Введите токен: ")
            try:
                vk_session = vk_api.VkApi(token=token)
                vk = vk_session.get_api()
                test = vk.users.get()
                print("Авторизация по введенному токену успешна!")
                
                # Сохранение токена
                with open(token_file, "w") as f:
                    f.write(token)
                print(f"Токен сохранен в файл {token_file}")
                
                return vk
            except Exception as e:
                print(f"Ошибка при авторизации по введенному токену: {e}")
        
        return None


def get_chat_members(vk: vk_api.vk_api.VkApiMethod, chat_id: int) -> List[Dict[str, Any]]:
    """
    Получение списка участников беседы.
    
    Args:
        vk (vk_api.vk_api.VkApiMethod): API-клиент VK.
        chat_id (int): ID беседы.
        
    Returns:
        List[Dict[str, Any]]: Список участников беседы или пустой список при ошибке.
    """
    try:
        chat_info = vk.messages.getConversationMembers(peer_id=2000000000 + chat_id)
        return chat_info['items']
    except vk_api.exceptions.ApiError as e:
        print(f"Ошибка при получении участников беседы: {e}")
        print("Пожалуйста, убедитесь, что:")
        print("1. Вы являетесь участником этой беседы")
        print("2. У вас есть права на просмотр участников (если вы не администратор)")
        print("3. ID беседы указан правильно")
        return []


def get_user_info(vk: vk_api.vk_api.VkApiMethod, user_id: int) -> Optional[Dict[str, Any]]:
    """
    Получение информации о пользователе.
    
    Args:
        vk (vk_api.vk_api.VkApiMethod): API-клиент VK.
        user_id (int): ID пользователя.
        
    Returns:
        Optional[Dict[str, Any]]: Информация о пользователе или None при ошибке.
    """
    try:
        user_info = vk.users.get(
            user_ids=user_id, 
            fields="city,bdate,sex,education,universities,schools,followers_count,relation,occupation,status"
        )[0]
        return user_info
    except Exception as e:
        print(f"Ошибка при получении информации о пользователе {user_id}: {e}")
        return None


def get_user_friends(vk: vk_api.vk_api.VkApiMethod, user_id: int) -> List[int]:
    """
    Получение списка друзей пользователя.
    
    Args:
        vk (vk_api.vk_api.VkApiMethod): API-клиент VK.
        user_id (int): ID пользователя.
        
    Returns:
        List[int]: Список ID друзей пользователя или пустой список при ошибке.
    """
    try:
        friends = vk.friends.get(user_id=user_id)
        return friends['items']
    except Exception as e:
        print(f"Ошибка при получении друзей пользователя {user_id}: {e}")
        return []


def save_to_csv(data: List[Dict[str, Any]], filename: str = "vk_chat_members.csv") -> None:
    """
    Сохранение данных о пользователях в CSV файл.
    
    Args:
        data (List[Dict[str, Any]]): Список с информацией о пользователях.
        filename (str): Имя файла для сохранения данных.
        
    Returns:
        None
    """
    if not data:
        print("Нет данных для сохранения")
        return
    
    # Определение полей для CSV
    fieldnames = [
        'id', 'first_name', 'last_name', 'sex', 'bdate', 
        'city', 'education', 'university_name', 'followers_count', 
        'relation', 'occupation', 'status'
    ]
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for user in data:
            # Подготовка данных для записи
            user_data = {
                'id': user.get('id', ''),
                'first_name': user.get('first_name', ''),
                'last_name': user.get('last_name', ''),
                'sex': user.get('sex', ''),
                'bdate': user.get('bdate', ''),
                'city': user.get('city', {}).get('title', '') if 'city' in user else '',
                'education': user.get('education', ''),
                'university_name': user.get('university_name', ''),
                'followers_count': user.get('followers_count', ''),
                'relation': user.get('relation', ''),
                'occupation': user.get('occupation', {}).get('name', '') if 'occupation' in user else '',
                'status': user.get('status', '')
            }
            writer.writerow(user_data)
    
    print(f"Данные сохранены в файл {filename}")


def save_intermediate_data(
    users_data: List[Dict[str, Any]], 
    friends_data: Dict[int, List[int]], 
    prefix: str = "intermediate"
) -> None:
    """
    Сохранение промежуточных результатов в JSON.
    
    Args:
        users_data (List[Dict[str, Any]]): Данные о пользователях.
        friends_data (Dict[int, List[int]]): Словарь со списками друзей (ключ: ID пользователя, значение: список ID друзей).
        prefix (str): Префикс для имен файлов.
        
    Returns:
        None
    """
    if not os.path.exists("temp"):
        os.makedirs("temp")
    
    # Сохранение данных пользователей
    users_file = f"temp/{prefix}_users_data.json"
    with open(users_file, "w", encoding="utf-8") as f:
        json.dump(users_data, f, ensure_ascii=False, indent=2)
    
    # Сохранение данных о друзьях
    friends_file = f"temp/{prefix}_friends_data.json"
    with open(friends_file, "w", encoding="utf-8") as f:
        # Преобразуем ключи из int в str для JSON
        friends_data_str = {str(k): v for k, v in friends_data.items()}
        json.dump(friends_data_str, f, ensure_ascii=False, indent=2)
    
    print(f"Промежуточные данные сохранены в файлы {users_file} и {friends_file}")


def load_intermediate_data(prefix: str = "intermediate") -> Tuple[List[Dict[str, Any]], Dict[int, List[int]]]:
    """
    Загрузка промежуточных результатов из JSON.
    
    Args:
        prefix (str): Префикс для имен файлов.
        
    Returns:
        Tuple[List[Dict[str, Any]], Dict[int, List[int]]]: Кортеж с данными о пользователях и их друзьях.
    """
    users_data: List[Dict[str, Any]] = []
    friends_data: Dict[int, List[int]] = {}
    
    users_file = f"temp/{prefix}_users_data.json"
    friends_file = f"temp/{prefix}_friends_data.json"
    
    if os.path.exists(users_file) and os.path.exists(friends_file):
        try:
            # Загрузка данных пользователей
            with open(users_file, "r", encoding="utf-8") as f:
                users_data = json.load(f)
            
            # Загрузка данных о друзьях
            with open(friends_file, "r", encoding="utf-8") as f:
                friends_data_str = json.load(f)
                # Преобразуем ключи из str в int
                friends_data = {int(k): v for k, v in friends_data_str.items()}
            
            print(f"Загружены промежуточные данные: {len(users_data)} пользователей и {len(friends_data)} записей о друзьях")
        except Exception as e:
            print(f"Ошибка при загрузке промежуточных данных: {e}")
    
    return users_data, friends_data


def save_connections_csv(
    users_data: List[Dict[str, Any]], 
    friends_data: Dict[int, List[int]]
) -> None:
    """
    Сохранение данных о связях в CSV.
    
    Args:
        users_data (List[Dict[str, Any]]): Данные о пользователях.
        friends_data (Dict[int, List[int]]): Словарь со списками друзей.
        
    Returns:
        None
    """
    # Создаем множество для быстрого доступа к ID пользователей беседы
    chat_users_ids: Set[int] = {user['id'] for user in users_data}
    
    # Открываем файл для записи
    with open("vk_chat_connections.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["source_id", "target_id"])
        
        # Для каждого пользователя проверяем его друзей среди участников беседы
        for user_id, friends_ids in friends_data.items():
            # Находим пересечение между друзьями пользователя и участниками беседы
            friends_in_chat = set(friends_ids) & chat_users_ids
            
            # Записываем каждую связь (только в одну сторону)
            for friend_id in friends_in_chat:
                if user_id < friend_id:  # записываем каждую связь только один раз
                    writer.writerow([user_id, friend_id])
    
    print("Данные о связях сохранены в файл vk_chat_connections.csv")


def save_extended_connections(
    users_data: List[Dict[str, Any]], 
    friends_data: Dict[int, List[int]]
) -> None:
    """
    Сохранение расширенных данных о связях с именами пользователей.
    
    Args:
        users_data (List[Dict[str, Any]]): Данные о пользователях.
        friends_data (Dict[int, List[int]]): Словарь со списками друзей.
        
    Returns:
        None
    """
    # Создаем словарь для быстрого доступа к данным о пользователях
    users_dict: Dict[int, Dict[str, Any]] = {user['id']: user for user in users_data}
    
    # Получаем множество ID всех пользователей беседы
    chat_users_ids: Set[int] = set(users_dict.keys())
    
    # Сохранение данных о связях в расширенном формате
    with open("vk_chat_connections_extended.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "source_id", "source_first_name", "source_last_name", 
            "target_id", "target_first_name", "target_last_name", 
            "relationship_type"
        ])
        
        # Для каждого пользователя проверяем его друзей среди участников беседы
        for user_id, friends_ids in friends_data.items():
            if user_id not in users_dict:
                continue
                
            source_user = users_dict[user_id]
            
            # Находим пересечение между друзьями пользователя и участниками беседы
            friends_in_chat = set(friends_ids) & chat_users_ids
            
            # Записываем каждую связь с полными данными
            for friend_id in friends_in_chat:
                if user_id < friend_id:  # записываем каждую связь только один раз
                    if friend_id not in users_dict:
                        continue
                        
                    target_user = users_dict[friend_id]
                    
                    writer.writerow([
                        user_id, 
                        source_user.get('first_name', ''), 
                        source_user.get('last_name', ''),
                        friend_id, 
                        target_user.get('first_name', ''), 
                        target_user.get('last_name', ''),
                        "friend"
                    ])
    
    print("Расширенные данные о связях сохранены в файл vk_chat_connections_extended.csv")


def save_connections_stats(
    users_data: List[Dict[str, Any]], 
    friends_data: Dict[int, List[int]]
) -> None:
    """
    Сохранение статистики по связям каждого пользователя.
    
    Args:
        users_data (List[Dict[str, Any]]): Данные о пользователях.
        friends_data (Dict[int, List[int]]): Словарь со списками друзей.
        
    Returns:
        None
    """
    # Получаем множество ID всех пользователей беседы
    chat_users_ids: Set[int] = {user['id'] for user in users_data}
    
    # Сохранение статистики по связям
    with open("vk_chat_connections_stats.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "user_id", "first_name", "last_name", 
            "connections_count", "connections_within_chat", 
            "connection_ratio"
        ])
        
        for user in users_data:
            user_id = user['id']
            # Получаем список друзей пользователя (пустой список, если нет)
            user_friends = set(friends_data.get(user_id, []))
            
            # Количество друзей внутри беседы
            friends_in_chat = len(user_friends & chat_users_ids)
            # Общее количество друзей
            total_friends = len(user_friends)
            # Доля друзей из беседы среди всех друзей
            ratio = friends_in_chat / total_friends if total_friends > 0 else 0
            
            writer.writerow([
                user_id,
                user.get('first_name', ''),
                user.get('last_name', ''),
                total_friends,
                friends_in_chat,
                f"{ratio:.2f}"
            ])
    
    print("Статистика по связям сохранена в файл vk_chat_connections_stats.csv")


def collect_users_data(
    vk: vk_api.vk_api.VkApiMethod, 
    chat_id: int
) -> Tuple[List[Dict[str, Any]], Dict[int, List[int]]]:
    """
    Сбор данных о пользователях и их друзьях.
    
    Args:
        vk (vk_api.vk_api.VkApiMethod): API-клиент VK.
        chat_id (int): ID беседы.
        
    Returns:
        Tuple[List[Dict[str, Any]], Dict[int, List[int]]]: Кортеж с данными о пользователях и их друзьях.
    """
    users_data: List[Dict[str, Any]] = []
    friends_data: Dict[int, List[int]] = {}
    
    print("Получение списка участников беседы...")
    members = get_chat_members(vk, chat_id)
    
    if not members:
        print("Не удалось получить список участников беседы")
        return users_data, friends_data
    
    print(f"Найдено {len(members)} участников. Получение информации о пользователях...")
    
    try:
        for i, member in enumerate(members):
            user_id = member['member_id']
            
            # Проверяем, что ID положительный (не группа)
            if user_id > 0:
                user_info = get_user_info(vk, user_id)
                if user_info:
                    users_data.append(user_info)
                    
                    # Получение друзей пользователя
                    print(f"Получение списка друзей для пользователя {user_info['first_name']} {user_info['last_name']} ({i+1}/{len(members)})...")
                    friends = get_user_friends(vk, user_id)
                    friends_data[user_id] = friends
                    
                    # Сохраняем промежуточные результаты каждые 5 пользователей
                    if (i + 1) % 5 == 0:
                        save_intermediate_data(users_data, friends_data)
                    
                    # Задержка между запросами
                    time.sleep(0.5)
    except KeyboardInterrupt:
        print("\nПрерывание пользователем. Сохраняем промежуточные данные...")
        save_intermediate_data(users_data, friends_data)
        print("Можете продолжить работу позже, запустив скрипт заново.")
    except Exception as e:
        print(f"Произошла ошибка: {e}")
        print("Сохраняем промежуточные данные...")
        save_intermediate_data(users_data, friends_data)
        print("Можете продолжить работу позже, запустив скрипт заново.")
    
    return users_data, friends_data


def get_chat_id() -> int:
    """
    Получение ID беседы от пользователя или из сохраненного файла.
    
    Returns:
        int: ID беседы.
    """
    chat_id_file = "chat_id.txt"
    
    if os.path.exists(chat_id_file):
        with open(chat_id_file, "r") as f:
            saved_chat_id = f.read().strip()
            use_saved = input(f"Использовать сохраненный ID беседы ({saved_chat_id})? (да/нет): ")
            if use_saved.lower() in ["да", "д", "yes", "y"]:
                return int(saved_chat_id)
    
    # Если нет сохраненного ID или пользователь выбрал новый
    chat_id = int(input("Введите ID беседы (числовой идентификатор): "))
    
    # Сохраняем ID беседы для последующего использования
    with open(chat_id_file, "w") as f:
        f.write(str(chat_id))
    
    return chat_id


def main() -> None:
    """
    Основная функция программы.
    
    Returns:
        None
    """
    # Авторизуемся
    vk = auth_vk()
    if not vk:
        return
    
    # Определяем ID беседы
    chat_id = get_chat_id()
    
    # Проверяем наличие промежуточных данных
    users_data, friends_data = load_intermediate_data()
    
    if users_data and friends_data:
        use_saved = input("Найдены ранее собранные данные. Использовать их? (да/нет): ")
        if use_saved.lower() not in ["да", "д", "yes", "y"]:
            # Собираем данные заново
            users_data, friends_data = collect_users_data(vk, chat_id)
    else:
        # Собираем данные, так как нет сохраненных
        users_data, friends_data = collect_users_data(vk, chat_id)
    
    # Если данных нет или произошла ошибка при сборе
    if not users_data or not friends_data:
        print("Нет данных для сохранения. Завершение работы.")
        return
    
    # Сохранение всех данных в CSV-файлы
    save_to_csv(users_data, "vk_chat_members.csv")
    save_connections_csv(users_data, friends_data)
    save_extended_connections(users_data, friends_data)
    save_connections_stats(users_data, friends_data)
    
    print("Готово! Все данные сохранены.")


if __name__ == "__main__":
    main()

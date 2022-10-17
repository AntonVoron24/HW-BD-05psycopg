import psycopg2
import os
import dotenv


dotenv.load_dotenv()


def drop_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
                DROP TABLE IF EXISTS clients, phones CASCADE;
                """)
    print('Таблицы удалены')


def create_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS clients(
                id SERIAL PRIMARY KEY,
                first_name VARCHAR(30) NOT NULL,
                last_name VARCHAR(30) NOT NULL,
                email VARCHAR(30) NOT NULL UNIQUE          
                );
            CREATE TABLE IF NOT EXISTS phones(
                id SERIAL PRIMARY KEY,
                client_id INT REFERENCES clients(id),
                phone_number VARCHAR(30) UNIQUE        
                );
                """)
    print('Таблицы созданы')


def add_client(conn, first_name, last_name, email, *phone_numbers):
    """Функция добавления клиента в базу"""
    client = find_client(conn=conn, first_name=first_name, last_name=last_name, email=email)
    if not client:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO clients (first_name, last_name, email)
                VALUES (%s, %s, %s) RETURNING id;
                """, (first_name, last_name, email))
            client_id = cur.fetchone()
            print(f'Клиент {first_name} {last_name} добавлен с id', *client_id)
            if phone_numbers:
                for phone_number in list(phone_numbers):
                    add_phone(conn, *client_id, phone_number)
    elif email == list(client[0])[3]:
        return print(f'Клиент с {email} уже есть в базе')
    elif list(client[0])[4] in list(phone_numbers):
        return print('Клиент с телефоном', list(client[0])[4], 'уже есть в базе')


def add_phone(conn, client_id, phone_number):
    """ Функция добавления телефона в базу."""
    with conn.cursor() as cur:
        cur.execute("""
                INSERT INTO phones (client_id, phone_number)
                VALUES (%s, %s);
                """, (client_id, phone_number))
        print(f'Телефон {phone_number} добавлен к клиенту c id {client_id}')


def change_client(conn, search=None, client_id=None, first_name=None, last_name=None, email=None, phone_number=None):
    """ Функция изменения данных о клиенте.
    Производит поиск по client_id.
    Если не знаешь id клиента, можно его найти по телефону или email, передав искомое значение в параметр search.
    Для изменения номера телефона необходимо в параметр search ввести номер, который необходимо заменить
    """
    with conn.cursor() as cur:
        if client_id:
            id = client_id
        else:
            email_search = find_client(conn=conn, email=search)
            phone_search = find_client(conn=conn, phone_number=search)

            if email_search:
                id = list(*email_search)[0]
            elif phone_search:
                id = list(*phone_search)[0]
            else:
                print('Клиент с указанными параметрами поиска не найдено')
                print('Поиск осуществляется по номеру телефона или email')
                return

        if first_name:
            cur.execute("""
                        UPDATE clients SET first_name=%s WHERE id=%s;
                        """, (first_name, id))
            conn.commit()
            print(f'Имя клиента ID{id} изменено --> {first_name}')
        if last_name:
            cur.execute("""
                        UPDATE clients SET last_name=%s WHERE id=%s;
                        """, (last_name, id))
            conn.commit()
            print(f'Фамилия клиента ID{id} изменена --> {last_name}')
        if email:
            cur.execute("""
                        UPDATE clients SET email=%s WHERE id=%s;
                        """, (email, id))
            conn.commit()
            print(f'Email клиента ID{id} изменен --> {email}')
        if phone_number:
            if not search:
                print('В параметр search не передан телефон')
            else:
                cur.execute("""
                            UPDATE phones SET phone_number=%s WHERE phone_number=%s;
                            """, (phone_number, search))
                conn.commit()
                print(f'Телефон {search} изменен на {phone_number}')


def del_phone(conn, phone_number):
    """
    Функция удаления одного телефона из базы.
    """
    cur = conn.cursor()
    phone_search = find_client(conn=conn, phone_number=phone_number)
    if not phone_search:
        print('Телефон не найден. Проверь корректность ввода')
        return
    else:
        phone = list(*phone_search)[4]
        cur.execute("""
                DELETE FROM phones WHERE phone_number=%s;
                """, (str(phone)))
        conn.commit()
        print(f'Телефон {phone} удален')


def del_all_phones_by_id(conn, client_id):
    """
    Функция удаления всех телефонов, принадлежащие конкретному клиенту (поиск по ID)
    """
    cur = conn.cursor()
    id_search = find_client(conn=conn, client_id=client_id)
    if not id_search:
        print('Клиент не найден. Проверь корректность ввода')
        return 0

    elif list(id_search[0])[4] is None:
        print(f'У клиента ID{client_id} отсутствуют телефоны')
    else:
        cur.execute("""
                DELETE FROM phones WHERE client_id=%s;
                """, (str(client_id)))
        conn.commit()
        print(f'Все телефоны клиента ID{client_id} удалены')


def del_client(conn, client_id):
    with conn.cursor() as cur:
        phone_del = del_all_phones_by_id(conn, client_id)
        if phone_del == 0:
            return
        cur.execute("""
                DELETE FROM clients WHERE id=%s;
                """, (str(client_id)))
        conn.commit()
        print(f'Все данные клиента ID{client_id} удалены')

def find_client(conn, client_id=None, first_name=None, last_name=None, email=None, phone_number=None):
    with conn.cursor() as cur:
        if phone_number:  # Проверяем был ли введен телефон
            cur.execute("""
                        SELECT c.id, first_name, last_name, email, phone_number FROM phones p 
                        JOIN clients c ON p.client_id=c.id
                        WHERE p.phone_number=%s
                        ;
                        """, (phone_number,))

            return cur.fetchall()
        else:
            cur.execute("""
                    SELECT c.id, first_name, last_name, email, phone_number FROM clients c
                    LEFT JOIN phones p ON c.id=p.client_id
                    WHERE c.id=%s OR c.first_name=%s OR c.last_name=%s OR c.email=%s
                    ;
                    """, (client_id, first_name, last_name, email))
            return cur.fetchall()


#TODO создать dotenv файл и сохранить пароль
with psycopg2.connect(database="clientsDB", user="postgres", password=os.getenv('password')) as conn:

    drop_table(conn)  # Удаление таблиц
    create_db(conn)  # Создание таблиц БД

    # # Создание клиента без номера телефона
    add_client(conn, 'Иван', 'Иванов', 'ii@mail.ru')
    # Создание клиента c 1 номером телефона
    add_client(conn, 'Петр', 'Петров', 'pp@mail.ru', '+782487623')
    add_client(conn, 'Петр', 'Петров', 'pp@mail.ru', '+782487623')  # тест дубля по email
    # Создание клиента c 2 номерами телефонов
    add_client(conn, 'Алексей', 'Алексеев', 'aa@mail.ru', '+79992736478', '+34587278495')
    add_client(conn, 'Алексей', 'Алексеев', 'aaa@mail.ru', '+79992736478', '+34587278495')  # тест дубля по телефонам

    # Поиск клиента
    client = find_client(conn, email='ii@mail.ru')
    print(f'Результат поиска по email --> {list(client[0])}')

    change_client(conn, search='aii@mail.ru', last_name='a')  # Тест на изменение не существующего клиента
    change_client(conn, client_id=2, first_name='Денис', last_name='Суровый')  # Изменение имени и фамилии
    change_client(conn, search='+79992736478', phone_number='88005553535')  # Изменение телефона
    change_client(conn, search='+7999', phone_number='88005553535')  # Изменение телефона которого нет в базе
    change_client(conn, search='абракадабра', phone_number='88005553535')  # В параметр search не передан телефон
    change_client(conn, client_id=2, phone_number='88005553535')  # search не применен для изменения телефона
    del_all_phones_by_id(conn, 999)  # Тест удаления всех телефонов несуществующего клиента
    del_all_phones_by_id(conn, 1)  # Тест удаления всех телефонов клиента без телефона
    del_all_phones_by_id(conn, 3)  # Удаление всех телефонов у клиента с 2 телефонами
    del_client(conn, 2)  # Удаление клиента с телефоном
    del_client(conn, 1)  # Удаление клиента без телефона


conn.close()
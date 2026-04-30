from klass import DatabaseManager
import os

print("=== MySQL Full Integration Test ===")


try:
    with DatabaseManager(db_type='mysql') as db:
        
 
        db.execute_query("""
            CREATE TABLE IF NOT EXISTS test_mysql_table (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100),
                age INT,
                status VARCHAR(50)
            )
        """)
        
     
        db.execute_query("TRUNCATE TABLE test_mysql_table")


        db.create('test_mysql_table', {'name': 'Ivan', 'age': 25, 'status': 'active'})
        db.create('test_mysql_table', {'name': 'Maria', 'age': 18, 'status': 'pending'})
        db.create('test_mysql_table', {'name': 'Petr', 'age': 40, 'status': 'active'})
        db.create('test_mysql_table', {'name': 'Elena', 'age': 30, 'status': 'banned'})

        print("\n1. --- Сортировка (Имена по алфавиту) ---")
        sorted_names = db.get_sorted_column('test_mysql_table', 'name', ascending=True)
        for row in sorted_names:
            print(row)

        print("\n2. --- Диапазон строк по ID (от 2 до 4) ---")
        range_rows = db.get_range_by_id('test_mysql_table', 2, 4)
        print(f"Получено строк: {len(range_rows)}")

        print("\n3. --- Вывод структуры таблицы ---")
   
        structure = db.get_table_structure('test_mysql_table')
        for column in structure:
            print(f"Поле: {column['Field']}, Тип: {column['Type']}, NULL: {column['Null']}")

        print("\n4. --- Поиск конкретного значения (age = 40) ---")
        specific_user = db.find_by_value('test_mysql_table', 'age', 40)
        print(specific_user)

        print("\n5. --- Работа с колонками (Добавление и удаление 'last_login') ---")
        db.add_column('test_mysql_table', 'last_login', 'DATETIME')
      
        if any(col['Field'] == 'last_login' for col in db.get_table_structure('test_mysql_table')):
            print("✅ Колонка добавлена")
        
        db.drop_column('test_mysql_table', 'last_login')
        print("✅ Колонка удалена")

        print("\n6. --- Экспорт в CSV ---")
        filename = "mysql_export.csv"
        db.export_to_csv('test_mysql_table', filename)

        print("\n7. --- Импорт из CSV (в новую таблицу) ---")
   
        db.execute_query("CREATE TABLE IF NOT EXISTS test_mysql_import LIKE test_mysql_table")
        db.execute_query("TRUNCATE TABLE test_mysql_import")
        db.import_from_csv('test_mysql_import', filename)

        print("\n8. --- Удаление диапазона (ID 1 и 2) ---")
        affected = db.delete_range_by_id('test_mysql_table', 1, 2)
        print(f"Удалено записей: {affected}")

        print("\n9. --- Проверка UNION (твоя старая функция) ---")
        union_res = db.union([
            "SELECT name FROM test_mysql_table WHERE age < 30",
            "SELECT name FROM test_mysql_table WHERE age >= 30"
        ])
        print(f"Результат UNION: {union_res}")

        print("\n10. --- Удаление таблиц (Очистка) ---")
        db.drop_table('test_mysql_import')
        db.drop_table('test_mysql_table')

    
        if os.path.exists(filename):
            os.remove(filename)

    print("\n✅ MySQL тест успешно завершён!")

except Exception as e:
    print(f"\n❌ Ошибка в процессе тестирования MySQL: {e}")
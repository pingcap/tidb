#!/usr/bin/env python3
"""
脚本功能：
1. 创建db1-db8数据库
2. 从test数据库中随机选取100张表，在db1-8中创建相同结构的表
3. 使用tiup dumpling导出选中表的表结构
"""

import mysql.connector
import random
import subprocess
import sys
from typing import List, Tuple

# 数据库连接配置
DB_CONFIG = {
    'host': '127.0.0.1',
    'port': 4000,
    'user': 'root',
    'password': '',
    'database': 'test'
}

def connect_db():
    """连接数据库"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"连接数据库失败: {e}")
        sys.exit(1)

def create_databases(conn) -> None:
    """创建db1-db8数据库"""
    cursor = conn.cursor()
    
    print("正在创建数据库db1-db8...")
    for i in range(1, 9):
        db_name = f"db{i}"
        try:
            cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
            cursor.execute(f"CREATE DATABASE {db_name}")
            print(f"创建数据库 {db_name} 成功")
        except Exception as e:
            print(f"创建数据库 {db_name} 失败: {e}")
    
    cursor.close()

def get_all_tables(conn) -> List[str]:
    """获取test数据库中的所有表"""
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES")
    tables = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return tables

def select_random_tables(tables: List[str], count: int = 100) -> List[str]:
    """随机选择指定数量的表"""
    if len(tables) < count:
        print(f"警告: 数据库中只有 {len(tables)} 张表，少于所需的 {count} 张")
        return tables
    
    selected_tables = random.sample(tables, count)
    print(f"随机选择了 {len(selected_tables)} 张表")
    return selected_tables

def get_table_structure(conn, table_name: str) -> str:
    """获取表的创建语句"""
    cursor = conn.cursor()
    try:
        cursor.execute(f"SHOW CREATE TABLE test.{table_name}")
        result = cursor.fetchone()
        cursor.close()
        if result:
            return result[1]  # CREATE TABLE语句
    except Exception as e:
        print(f"获取表 {table_name} 结构失败: {e}")
        cursor.close()
    return ""

def copy_tables_to_databases(conn, tables: List[str]) -> List[Tuple[str, str]]:
    """将选中的表结构复制到db1-8数据库中"""
    cursor = conn.cursor()
    created_tables = []
    
    print("正在复制表结构到目标数据库...")
    for i, table in enumerate(tables):
        # 随机选择目标数据库 (db1-db8)
        target_db = f"db{random.randint(1, 8)}"
        # 保持原表名
        new_table_name = table
        
        try:
            # 获取原表的创建语句
            create_sql = get_table_structure(conn, table)
            if not create_sql:
                print(f"跳过表 {table} - 无法获取结构")
                continue
                
            # 修改CREATE语句中的表名，添加目标数据库
            modified_sql = create_sql.replace(f'CREATE TABLE `{table}`', 
                                             f'CREATE TABLE `{target_db}`.`{new_table_name}`')
            
            # 在目标数据库中创建表
            cursor.execute(modified_sql)
            created_tables.append((target_db, new_table_name))
            print(f"在 {target_db} 中创建表 {new_table_name} 成功")
            
            # 限制创建的表数量，避免过多错误
            if len(created_tables) >= 20:
                break
                
        except Exception as e:
            print(f"复制表 {table} 到 {target_db} 失败: {e}")
    
    cursor.close()
    return created_tables

def generate_dumpling_command(created_tables: List[Tuple[str, str]]) -> str:
    """生成tiup dumpling命令"""
    table_list = []
    for db, table in created_tables:
        table_list.append(f"{db}.{table}")
    
    tables_str = ",".join(table_list)
    # 添加完整的dumpling参数
    command = f"tiup dumpling -h 127.0.0.1 -P 4000 -u root --no-data -T {tables_str} -o ./export-schema-only"
    return command

def run_dumpling(command: str) -> None:
    """执行tiup dumpling命令"""
    print(f"执行命令: {command}")
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            print("导出成功!")
            print("输出:", result.stdout)
        else:
            print("导出失败!")
            print("错误:", result.stderr)
    except Exception as e:
        print(f"执行命令失败: {e}")

def show_summary(created_tables: List[Tuple[str, str]]) -> None:
    """显示创建的表的摘要信息"""
    print("\n=== 创建表摘要 ===")
    db_count = {}
    for db, table in created_tables:
        db_count[db] = db_count.get(db, 0) + 1
    
    for db, count in sorted(db_count.items()):
        print(f"{db}: {count} 张表")
    
    print(f"\n总共创建了 {len(created_tables)} 张表")
    print("\n创建的表列表:")
    for db, table in created_tables:
        print(f"  {db}.{table}")

def main():
    print("开始执行表复制和导出脚本...")
    
    # 连接数据库
    conn = connect_db()
    
    try:
        # 1. 创建db1-db8数据库
        create_databases(conn)
        
        # 2. 获取所有表
        all_tables = get_all_tables(conn)
        print(f"test数据库中共有 {len(all_tables)} 张表")
        
        # 3. 随机选择100张表
        selected_tables = select_random_tables(all_tables, 100)
        
        # 4. 复制表结构到不同数据库
        created_tables = copy_tables_to_databases(conn, selected_tables)
        
        if created_tables:
            # 显示摘要
            show_summary(created_tables)
            
            # 5. 生成并执行dumpling命令
            dumpling_cmd = generate_dumpling_command(created_tables)
            print(f"\n准备执行dumpling导出命令:")
            print(f"{dumpling_cmd}\n")
            
            # 自动执行dumpling命令
            print("正在执行dumpling导出...")
            run_dumpling(dumpling_cmd)
        else:
            print("没有成功创建任何表")
        
    except Exception as e:
        print(f"脚本执行过程中出错: {e}")
    finally:
        conn.close()
        print("脚本执行完成")

if __name__ == "__main__":
    main()

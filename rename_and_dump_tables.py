#!/usr/bin/env python3
"""
脚本功能：
1. 创建db1-db8数据库
2. 从test数据库中随机选取100张表，随机分配到db1-8中并重命名
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

def rename_tables_to_databases(conn, tables: List[str]) -> List[Tuple[str, str]]:
    """将选中的表随机重命名并移动到db1-8数据库中"""
    cursor = conn.cursor()
    renamed_tables = []
    
    print("正在重命名并移动表...")
    for i, table in enumerate(tables):
        # 随机选择目标数据库 (db1-db8)
        target_db = f"db{random.randint(1, 8)}"
        # 保持原表名或生成新表名
        new_table_name = table
        
        try:
            # 使用RENAME TABLE语句移动表
            sql = f"RENAME TABLE test.{table} TO {target_db}.{new_table_name}"
            cursor.execute(sql)
            renamed_tables.append((target_db, new_table_name))
            print(f"移动表 {table} -> {target_db}.{new_table_name}")
        except Exception as e:
            print(f"移动表 {table} 失败: {e}")
    
    cursor.close()
    return renamed_tables

def generate_dumpling_command(renamed_tables: List[Tuple[str, str]]) -> str:
    """生成tiup dumpling命令"""
    table_list = []
    for db, table in renamed_tables:
        table_list.append(f"{db}.{table}")
    
    tables_str = ",".join(table_list)
    command = f"tiup dumpling -T {tables_str}"
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

def main():
    print("开始执行表重命名和导出脚本...")
    
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
        
        # 4. 重命名并移动表到不同数据库
        renamed_tables = rename_tables_to_databases(conn, selected_tables)
        
        if renamed_tables:
            # 5. 生成并执行dumpling命令
            dumpling_cmd = generate_dumpling_command(renamed_tables)
            print(f"\n准备执行dumpling导出命令:")
            print(f"{dumpling_cmd}\n")
            
            # 询问是否执行
            response = input("是否执行dumpling导出? (y/n): ")
            if response.lower() in ['y', 'yes']:
                run_dumpling(dumpling_cmd)
            else:
                print("跳过dumpling导出")
                print(f"手动执行命令: {dumpling_cmd}")
        
    except Exception as e:
        print(f"脚本执行过程中出错: {e}")
    finally:
        conn.close()
        print("脚本执行完成")

if __name__ == "__main__":
    main()

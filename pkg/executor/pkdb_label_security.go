// Copyright 2023-2023 PingCAP, Inc.

package executor

const (
	// The procedures of label scecurity are created in schema `LABELSECURITY_SCHEMA`.
	// We should grant all privilege of LABELSECURITY_SCHEMA to admin role when enabling LBAC.
	dropLabelSecuritySchema   = `DROP DATABASE IF EXISTS LABELSECURITY_SCHEMA;`
	createLabelSecuritySchema = `CREATE DATABASE IF NOT EXISTS LABELSECURITY_SCHEMA;`

	dropCreatePolicy = `drop procedure if exists LABELSECURITY_SCHEMA.create_policy;`
	createPolicy     = `create procedure if not exists LABELSECURITY_SCHEMA.create_policy(p_policy_name varchar(64), label_column varchar(64))
    begin
      INSERT INTO mysql.tidb_ls_policies values(p_policy_name, LOWER(label_column));
    end;`
	dropDropPolicy = `drop procedure if exists LABELSECURITY_SCHEMA.drop_policy;`
	// The procedure body is ugly, because we don't support `select into vars`, `cursor is open`, `raise error` ...,
	// It can't define the procedure gracefully.
	// in the future, we can use SQL statments to manage LBAC instead of porcedure.
	dropPolicy = `create procedure if not exists LABELSECURITY_SCHEMA.drop_policy(p_policy_name varchar(64))
	begin
		declare lb_column, p_schema_name, p_table_name varchar(64) default '';
		declare cur_lbcol_opened,cur_table_opened int default 0;
		declare cur_lbcol cursor for select label_column from mysql.tidb_ls_policies where policy_name = p_policy_name;
		declare cur_table cursor for select schema_name, table_name from mysql.tidb_ls_tables where policy_name = p_policy_name;
		DECLARE EXIT HANDLER FOR NOT FOUND select concat('There is no data about ', p_policy_name, ', please delete the remianed label manualy') as 'Warning Message'; 
	
		DECLARE EXIT HANDLER FOR SQLEXCEPTION 
		BEGIN               
			if cur_lbcol_opened = 1 then
				close cur_lbcol;
			end if;
			if cur_table_opened = 1 then
				close cur_table;
			end if;
		END;
		
		open cur_lbcol;
		set cur_lbcol_opened = 1;
		fetch cur_lbcol into lb_column; 
		close cur_lbcol;
		set cur_lbcol_opened = 0;
		
		start transaction;
		delete from mysql.tidb_ls_elements where policy_name = p_policy_name;    
		delete from mysql.tidb_ls_users where policy_name = p_policy_name;
		delete from mysql.tidb_ls_policies where policy_name = p_policy_name;
		commit;
		
		open cur_table;
		set cur_table_opened = 1;
		fetch cur_table into p_schema_name, p_table_name; 
		close cur_table;
		set cur_table_opened = 0;
		delete from mysql.tidb_ls_tables where policy_name = p_policy_name and schema_name = p_schema_name and table_name = p_table_name;
		
		set @sqlstr = concat("alter table ", p_schema_name, ".", p_table_name, " drop column if exists ", lb_column, ";");
		prepare stmt from @sqlstr;  
		execute stmt;
		drop prepare stmt;
	end;`

	dropCreateLevel = `drop procedure if exists LABELSECURITY_SCHEMA.create_level;`
	createLevel     = `create procedure if not exists LABELSECURITY_SCHEMA.create_level(p_policy_name varchar(64), 
		level_grade int,
		level_name varchar(64),
		level_comment varchar(128))
	begin
		INSERT INTO mysql.tidb_ls_elements values(p_policy_name, 'level', level_grade, level_name, level_comment);
	end;`

	dropDropLevel = `drop procedure if exists LABELSECURITY_SCHEMA.drop_level;`
	dropLevel     = `create procedure if not exists LABELSECURITY_SCHEMA.drop_level(p_policy_name varchar(64),
    	level_name varchar(64))
	begin
		declare cnt1, cnt2 int default 0;
		declare opened_tabs, opened_users int default 0;
		declare cur_tabs cursor for select count(1) from mysql.tidb_ls_tables where policy_name = p_policy_name;
		declare cur_users cursor for select count(1) from mysql.tidb_ls_users where policy_name = p_policy_name;
		
		DECLARE EXIT HANDLER FOR SQLEXCEPTION 
		BEGIN
			if opened_tabs = 1 then
				close cur_tabs;
			end if;
			if opened_users = 1 then        
				close cur_users;
			end if;
		END;

		open cur_tabs;
		set opened_tabs = 1;
		open cur_users;
		set opened_users = 1;
		fetch cur_tabs into cnt1; 
		fetch cur_users into cnt2;

		if cnt1 = 0 and cnt2 = 0 then
			delete from mysql.tidb_ls_elements where policy_name = p_policy_name and element_type = 'level' and element_name = level_name;        
		else 
		select concat('The level cannot be dropped, it is using by ',p_policy_name) as 'Warning Message'; 
		end if;
		
		close cur_tabs;
		set opened_tabs = 0;
		close cur_users;	
		set opened_users = 0;
	end;`

	dropCreateCompart = `drop procedure if exists LABELSECURITY_SCHEMA.create_compartment;`
	createCompart     = `create procedure if not exists LABELSECURITY_SCHEMA.create_compartment(p_policy_name varchar(64), 
		cpt_id int,
		cpt_name varchar(64),
		cpt_comment varchar(128))
	begin
		insert into mysql.tidb_ls_elements values(p_policy_name, 'compartment', cpt_id , cpt_name , cpt_comment);
	end;`
	dropDropCompart = `drop procedure if exists LABELSECURITY_SCHEMA.drop_compartment;`
	dropCompart     = `create procedure if not exists LABELSECURITY_SCHEMA.drop_compartment(p_policy_name varchar(64),
    	cpt_name varchar(64))
	begin
		declare cnt1, cnt2 int default 0;
		declare opened_tabs, opened_users int default 0;
		declare cur_tabs cursor for select count(1) from mysql.tidb_ls_tables where policy_name = p_policy_name;
		declare cur_users cursor for select count(1) from mysql.tidb_ls_users where policy_name = p_policy_name;
		
		DECLARE EXIT HANDLER FOR SQLEXCEPTION 
		BEGIN
			if opened_tabs = 1 then
				close cur_tabs;
			end if;
			if opened_users = 1 then        
				close cur_users;
			end if;
		END;

		open cur_tabs;
		set opened_tabs = 1;
		open cur_users;
		set opened_users = 1;
		fetch cur_tabs into cnt1; 
		fetch cur_users into cnt2;

		if cnt1 = 0 and cnt2 = 0 then
			delete from mysql.tidb_ls_elements where policy_name = p_policy_name and element_type = 'compartment' and element_name = cpt_name;        
		else 
		select concat('The compartment cannot be dropped, it is using by ',p_policy_name) as 'Warning Message'; 
		end if;
		
		close cur_tabs;
		set opened_tabs = 0;
		close cur_users;	
		set opened_users = 0;
	end;`

	dropCreateGroup = `drop procedure if exists LABELSECURITY_SCHEMA.create_group;`
	createGroup     = `create procedure if not exists LABELSECURITY_SCHEMA.create_group(p_policy_name varchar(64), 
		group_id int,
		group_name varchar(64),
		group_comment varchar(128))
	begin
		insert into mysql.tidb_ls_elements values(p_policy_name, 'group', group_id , group_name , group_comment);
	end;`
	dropDropGroup = `drop procedure if exists LABELSECURITY_SCHEMA.drop_group;`
	dropGroup     = `create procedure if not exists LABELSECURITY_SCHEMA.drop_group(p_policy_name varchar(64),
    	grp_name varchar(64))
	begin
		declare cnt1, cnt2 int default 0;
		declare opened_tabs, opened_users int default 0;
		declare cur_tabs cursor for select count(1) from mysql.tidb_ls_tables where policy_name = p_policy_name;
		declare cur_users cursor for select count(1) from mysql.tidb_ls_users where policy_name = p_policy_name;
		
		DECLARE EXIT HANDLER FOR SQLEXCEPTION 
		BEGIN
			if opened_tabs = 1 then
				close cur_tabs;
			end if;
			if opened_users = 1 then        
				close cur_users;
			end if;
		END;

		open cur_tabs;
		set opened_tabs = 1;
		open cur_users;
		set opened_users = 1;
		fetch cur_tabs into cnt1; 
		fetch cur_users into cnt2;

		if cnt1 = 0 and cnt2 = 0 then
			delete from mysql.tidb_ls_elements where policy_name = p_policy_name and element_type = 'group' and element_name = grp_name;        
		else 
		select concat('The group cannot be dropped, it is using by ',p_policy_name) as 'Warning Message'; 
		end if;
		
		close cur_tabs;
		set opened_tabs = 0;
		close cur_users;	
		set opened_users = 0;
	end;`

	dropSetUserLabel = `drop procedure if exists LABELSECURITY_SCHEMA.set_user_labels;`
	setUserLabel     = `create procedure if not exists LABELSECURITY_SCHEMA.set_user_labels(p_policy_name varchar(64), 
		user_name varchar(64),
		label_value  varchar(256))
	begin   
		insert into mysql.tidb_ls_users values(p_policy_name, user_name , label_value);
	end;`
	dropDropUserLabel = `drop procedure if exists LABELSECURITY_SCHEMA.drop_user_labels;`
	dropUserLabel     = `create procedure if not exists LABELSECURITY_SCHEMA.drop_user_labels(p_policy_name varchar(64), 
    	p_user_name varchar(64))
	begin
		delete from mysql.tidb_ls_users where policy_name = p_policy_name and user_name = p_user_name;
	end;`

	dropApplyTablePolicy = `drop procedure if exists LABELSECURITY_SCHEMA.apply_table_policy;`
	applyTablePolicy     = `create procedure if not exists LABELSECURITY_SCHEMA.apply_table_policy (p_policy_name varchar(64), 
		p_schema_name varchar(64),
		p_table_name  varchar(64),
		table_options  varchar(128))
	begin
		declare lb_column varchar(256) default '';
		declare cur_opened, inserted int default 0;
		declare cur_lbcol cursor for select label_column from mysql.tidb_ls_policies where policy_name = p_policy_name;
		DECLARE EXIT HANDLER FOR SQLEXCEPTION 
		BEGIN
		delete from mysql.tidb_ls_tables where policy_name = p_policy_name and schema_name = p_schema_name and table_name = p_table_name;               
		if cur_opened = 1 then
		close cur_lbcol;
		end if;
		END;

		open cur_lbcol;
		set cur_opened = 1;
		fetch cur_lbcol into lb_column; 
		close cur_lbcol;
		set cur_opened = 0;

		insert into mysql.tidb_ls_tables values(p_policy_name, p_schema_name, p_table_name, table_options);
		set inserted = 1;

		set @sqlstr = concat("alter table ", p_schema_name, ".", p_table_name, " add column ", lb_column, " varchar(256);");
		prepare stmt from @sqlstr;  
		execute stmt;
		drop prepare stmt;
	end;`
	dropRemoveTablePolicy = `drop procedure if exists LABELSECURITY_SCHEMA.remove_table_policy;`
	removeTablePolicy     = `create procedure if not exists LABELSECURITY_SCHEMA.remove_table_policy(p_policy_name varchar(64), 
		p_schema_name        varchar(64),
		p_table_name         varchar(256))
	begin
		declare lb_column varchar(256) default '';
		declare cur_opened int default 0;
		declare cur_lbcol cursor for select label_column from mysql.tidb_ls_policies where policy_name = p_policy_name;
		DECLARE EXIT HANDLER FOR SQLEXCEPTION 
		BEGIN
		if cur_opened = 1 then
		close cur_lbcol;
		end if;
		END;

		open cur_lbcol;
		set cur_opened = 1;
		fetch cur_lbcol into lb_column; 
		close cur_lbcol;
		set cur_opened = 0;

		delete from mysql.tidb_ls_tables where policy_name = p_policy_name and schema_name = p_schema_name  and table_name = p_table_name;

		set @sqlstr = concat("alter table ", p_schema_name, ".", p_table_name, " drop column ", lb_column, ";");
		prepare stmt from @sqlstr;  
		execute stmt;
		drop prepare stmt;
	end;`
)

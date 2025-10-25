# test composite string primary key - unicode and emoji characters
create table `comp_str_case_3` (
  lang_code varchar(10),
  message_id varchar(50),
  content text,
  primary key (lang_code, message_id)
);

insert into `comp_str_case_3` values
('en', 'welcome', 'Welcome to our application! 😊'),
('en', 'goodbye', 'Thank you for using our service'),
('zh', 'welcome', '欢迎使用我们的应用程序！'),
('zh', 'goodbye', '感谢您使用我们的服务'),
('ja', 'welcome', 'アプリケーションへようこそ！'),
('ja', 'goodbye', 'サービスをご利用いただきありがとうございます'),
('es', 'welcome', '¡Bienvenido a nuestra aplicación!'),
('es', 'goodbye', 'Gracias por usar nuestro servicio'),
('fr', 'welcome', 'Bienvenue dans notre application !'),
('fr', 'goodbye', 'Merci d''utiliser notre service'),
('de', 'welcome', 'Willkommen in unserer Anwendung!'),
('ar', 'welcome', 'مرحبا بكم في تطبيقنا!');
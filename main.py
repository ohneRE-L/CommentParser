#!/usr/bin/env python3
"""
Улучшенный мониторинг комментариев из социальных сетей
Включает: параллельную обработку, оптимизации, метрики, graceful shutdown
"""

import os
import asyncio
import logging
import platform
import json
import signal
from datetime import datetime
from typing import List, Dict, Optional
from collections import defaultdict

# Импортируем улучшенные парсеры
from vk_parser import create_vk_parser, Comment
from youtube_parser import create_youtube_parser, YouTubeQuotaExceeded
from reddit_parser import create_reddit_parser
from telegram import Bot

# ============================================================================
# КОНФИГУРАЦИЯ
# ============================================================================

def detect_environment():
    """Автоматически определяет окружение"""
    if os.path.exists('/etc/systemd/system/comment-monitor.service'):
        return 'server'
    elif platform.system() == 'Linux' and os.getenv('USER') == 'root':
        return 'server'
    else:
        return 'local'

ENVIRONMENT = detect_environment()

CONFIG = {
    'local': {
        'check_interval': 30,
        'log_level': 'INFO',
    },
    'server': {
        'check_interval': 30,
        'log_level': 'INFO',
    }
}

CURRENT_CONFIG = CONFIG.get(ENVIRONMENT, CONFIG['local'])
CHECK_INTERVAL = CURRENT_CONFIG['check_interval']
LOG_LEVEL = CURRENT_CONFIG['log_level']

LOG_FILE = 'comments_monitor_improved.log'
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

def load_config():
    """
    Загружает конфигурацию из файла config.txt или переменных окружения.
    Приоритет: переменные окружения > config.txt
    """
    config = {}
    
    # Список всех возможных ключей конфигурации
    config_keys = [
        'ENABLE_YOUTUBE', 'ENABLE_VK', 'ENABLE_REDDIT',
        'YOUTUBE_API_KEY', 'YOUTUBE_CHANNEL_ID',
        'VK_ACCESS_TOKEN', 'VK_GROUP_ID', 'VK_GROUP_URL',
        'REDDIT_CLIENT_ID', 'REDDIT_CLIENT_SECRET', 'REDDIT_USER_AGENT', 'REDDIT_SUBREDDITS',
        'TELEGRAM_BOT_TOKEN', 'TELEGRAM_GROUP_ID',
        'TELEGRAM_TOPIC_YOUTUBE', 'TELEGRAM_TOPIC_VK', 'TELEGRAM_TOPIC_REDDIT', 'TELEGRAM_TOPIC_ERRORS'
    ]
    
    # Сначала загружаем из переменных окружения
    for key in config_keys:
        env_value = os.getenv(key)
        if env_value is not None:
            config[key] = env_value
    
    # Затем загружаем из config.txt (переменные окружения имеют приоритет)
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config.txt')
    if os.path.exists('config.txt'):
        config_path = 'config.txt'
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    key = key.strip()
                    # Используем значение из файла только если его нет в переменных окружения
                    if key not in config:
                        config[key] = value.strip()
    except FileNotFoundError:
        # Файл не обязателен, если используются переменные окружения
        if not config:
            print("⚠️ Файл config.txt не найден и переменные окружения не установлены!")
            print("💡 Создайте config.txt на основе config.txt.example или установите переменные окружения")
    except Exception as e:
        print(f"❌ Ошибка загрузки конфигурации: {e}")
    
    return config

CONFIG_DATA = load_config()

def get_bool_config(key: str, default: bool = True) -> bool:
    """Читает булево значение из конфига"""
    value = CONFIG_DATA.get(key, str(default)).lower().strip()
    return value in ('true', '1', 'yes', 'on')

ENABLE_YOUTUBE = get_bool_config('ENABLE_YOUTUBE', True)
ENABLE_VK = get_bool_config('ENABLE_VK', True)
ENABLE_REDDIT = get_bool_config('ENABLE_REDDIT', True)

YOUTUBE_API_KEY = CONFIG_DATA.get('YOUTUBE_API_KEY', '')
YOUTUBE_CHANNEL_ID = CONFIG_DATA.get('YOUTUBE_CHANNEL_ID', '')

VK_ACCESS_TOKEN = CONFIG_DATA.get('VK_ACCESS_TOKEN', '')
VK_GROUP_ID = CONFIG_DATA.get('VK_GROUP_ID', '')
VK_GROUP_URL = CONFIG_DATA.get('VK_GROUP_URL', '')

REDDIT_CLIENT_ID = CONFIG_DATA.get('REDDIT_CLIENT_ID', '')
REDDIT_CLIENT_SECRET = CONFIG_DATA.get('REDDIT_CLIENT_SECRET', '')
REDDIT_USER_AGENT = CONFIG_DATA.get('REDDIT_USER_AGENT', '')
REDDIT_SUBREDDITS = CONFIG_DATA.get('REDDIT_SUBREDDITS', 'python').split(',')

TELEGRAM_BOT_TOKEN = CONFIG_DATA.get('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_GROUP_ID = CONFIG_DATA.get('TELEGRAM_GROUP_ID', '')
TELEGRAM_TOPICS = {
    'YouTube': int(CONFIG_DATA.get('TELEGRAM_TOPIC_YOUTUBE', '2')),
    'VK': int(CONFIG_DATA.get('TELEGRAM_TOPIC_VK', '4')),
    'Reddit': int(CONFIG_DATA.get('TELEGRAM_TOPIC_REDDIT', '6')),
    'Errors': int(CONFIG_DATA.get('TELEGRAM_TOPIC_ERRORS', '1'))  # Топик для ошибок
}

SOCIAL_NETWORKS = {
    'youtube': {
        'enabled': ENABLE_YOUTUBE,
        'api_key': YOUTUBE_API_KEY,
        'channel_id': YOUTUBE_CHANNEL_ID,
        'check_interval': 60
    },
    'vk': {
        'enabled': ENABLE_VK,
        'access_token': VK_ACCESS_TOKEN,
        'group_id': VK_GROUP_ID,
        'group_url': VK_GROUP_URL,
        'check_interval': 30
    },
    'reddit': {
        'enabled': ENABLE_REDDIT,
        'client_id': REDDIT_CLIENT_ID,
        'client_secret': REDDIT_CLIENT_SECRET,
        'user_agent': REDDIT_USER_AGENT,
        'subreddits': REDDIT_SUBREDDITS,
        'check_interval': 30
    }
}

# ============================================================================
# КЛАССЫ ОШИБОК
# ============================================================================

class ParserError(Exception):
    """Базовый класс для ошибок парсера"""
    pass

class ConfigError(ParserError):
    """Ошибка конфигурации"""
    pass

class APIError(ParserError):
    """Ошибка API"""
    pass

# YouTubeQuotaExceeded импортируется из youtube_parser.py

# ============================================================================
# ВАЛИДАЦИЯ КОНФИГУРАЦИИ
# ============================================================================

def validate_config():
    """Валидирует конфигурацию перед запуском"""
    errors = []
    
    if not TELEGRAM_BOT_TOKEN or TELEGRAM_BOT_TOKEN == 'YOUR_BOT_TOKEN_HERE':
        errors.append("TELEGRAM_BOT_TOKEN не установлен")
    if not TELEGRAM_GROUP_ID or TELEGRAM_GROUP_ID == 'YOUR_GROUP_ID_HERE':
        errors.append("TELEGRAM_GROUP_ID не установлен")
    
    if ENABLE_YOUTUBE and (not YOUTUBE_API_KEY or YOUTUBE_API_KEY == 'YOUR_YOUTUBE_API_KEY_HERE'):
        errors.append("YouTube парсер включен, но YOUTUBE_API_KEY не установлен")
    
    if ENABLE_VK and (not VK_ACCESS_TOKEN or VK_ACCESS_TOKEN == 'YOUR_VK_ACCESS_TOKEN_HERE'):
        errors.append("VK парсер включен, но VK_ACCESS_TOKEN не установлен")
    
    if ENABLE_REDDIT and (not REDDIT_CLIENT_ID or REDDIT_CLIENT_ID == 'YOUR_REDDIT_CLIENT_ID_HERE'):
        errors.append("Reddit парсер включен, но REDDIT_CLIENT_ID не установлен")
    
    if errors:
        raise ConfigError("Ошибки конфигурации:\n" + "\n".join(f"  - {e}" for e in errors))
    
    return True

# ============================================================================
# TELEGRAM ОТПРАВКА С БАТЧИНГОМ
# ============================================================================

class TelegramSender:
    """Класс для отправки сообщений в Telegram с оптимизациями"""
    
    def __init__(self):
        self.bot_token = TELEGRAM_BOT_TOKEN
        self.group_id = TELEGRAM_GROUP_ID
        self.topics = TELEGRAM_TOPICS
        self.bot = None
        self.logger = logging.getLogger("telegram")
        self._session = None
    
    async def setup_bot(self):
        """Настраивает бота"""
        try:
            self.logger.info(f"Подключение к Telegram боту...")
            
            import requests
            response = requests.get(f"https://api.telegram.org/bot{self.bot_token}/getMe", timeout=5)
            if response.status_code == 200:
                bot_data = response.json()
                if bot_data.get('ok'):
                    self.logger.info(f"Telegram API доступен: @{bot_data['result']['username']}")
                else:
                    self.logger.error(f"Telegram API ошибка: {bot_data}")
                    return False
            else:
                self.logger.error(f"HTTP ошибка: {response.status_code}")
                return False
            
            self.bot = Bot(token=self.bot_token)
            self.logger.info("Telegram бот инициализирован")
            return True
            
        except Exception as e:
            self.logger.error(f"Ошибка подключения к Telegram: {e}")
            return False
    
    async def send_comment(self, comment: Comment, source: str, max_retries: int = 3):
        """Отправляет один комментарий в Telegram с retry логикой"""
        if not self.bot:
            return
        
        message = f"💬 <b>{comment.author}</b>\n"
        message += f"📝 {comment.text[:200]}{'...' if len(comment.text) > 200 else ''}\n"
        message += f"🔗 {comment.source_url}\n"
        message += f"⏰ {comment.timestamp.strftime('%H:%M:%S')}"
        
        topic_id = None
        if source.lower() == "youtube":
            topic_id = self.topics.get('YouTube')
        elif source.lower() == "vk":
            topic_id = self.topics.get('VK')
        elif source.lower().startswith("reddit"):
            topic_id = self.topics.get('Reddit')
        
        # Retry логика для отправки в Telegram
        for attempt in range(max_retries):
            try:
                if topic_id:
                    await self.bot.send_message(
                        chat_id=self.group_id,
                        text=message,
                        parse_mode='HTML',
                        message_thread_id=topic_id
                    )
                else:
                    await self.bot.send_message(
                        chat_id=self.group_id,
                        text=message,
                        parse_mode='HTML'
                    )
                
                # Успешная отправка
                await asyncio.sleep(0.5)
                return
                
            except asyncio.TimeoutError:
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 2  # 2, 4, 6 секунд
                    self.logger.warning(
                        f"Таймаут отправки в Telegram для {comment.author}, повтор через {wait_time}с (попытка {attempt + 1}/{max_retries})"
                    )
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    self.logger.error(f"Таймаут отправки в Telegram для {comment.author} после {max_retries} попыток")
                    await asyncio.sleep(1)
                    return
                    
            except Exception as e:
                # Для других ошибок (не таймаут) тоже делаем retry
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 1.5  # 1.5, 3, 4.5 секунд
                    self.logger.warning(
                        f"Ошибка отправки в Telegram для {comment.author}: {e}, повтор через {wait_time}с (попытка {attempt + 1}/{max_retries})"
                    )
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    self.logger.error(f"Ошибка отправки в Telegram для {comment.author} после {max_retries} попыток: {e}")
                    await asyncio.sleep(1)
                    return
    
    async def send_comment_batch(self, comments: List[Comment], source: str):
        """Отправляет батч комментариев одним сообщением (опционально)"""
        if not comments or not self.bot:
            return
        
        # Для небольших батчей отправляем по одному
        if len(comments) <= 3:
            for comment in comments:
                await self.send_comment(comment, source)
            return
        
        # Для больших батчей можно отправить одним сообщением
        try:
            topic_id = None
            if source.lower() == "youtube":
                topic_id = self.topics.get('YouTube')
            elif source.lower() == "vk":
                topic_id = self.topics.get('VK')
            elif source.lower().startswith("reddit"):
                topic_id = self.topics.get('Reddit')
            
            message_parts = [f"💬 <b>{len(comments)} новых комментариев из {source}</b>\n\n"]
            for i, comment in enumerate(comments[:10], 1):  # Показываем первые 10
                message_parts.append(f"{i}. <b>{comment.author}</b>: {comment.text[:100]}{'...' if len(comment.text) > 100 else ''}\n")
            
            if len(comments) > 10:
                message_parts.append(f"\n... и еще {len(comments) - 10} комментариев")
            
            message = "".join(message_parts)
            
            if topic_id:
                await self.bot.send_message(
                    chat_id=self.group_id,
                    text=message,
                    parse_mode='HTML',
                    message_thread_id=topic_id
                )
            else:
                await self.bot.send_message(
                    chat_id=self.group_id,
                    text=message,
                    parse_mode='HTML'
                )
        except Exception as e:
            self.logger.error(f"Ошибка отправки батча в Telegram: {e}")
    
    async def send_error(self, error_message: str, parser_name: str = None, max_retries: int = 3):
        """Отправляет сообщение об ошибке в топик ошибок"""
        if not self.bot:
            self.logger.warning("Telegram бот не инициализирован, ошибка не отправлена")
            return
        
        topic_id = self.topics.get('Errors')
        self.logger.info(f"Отправка ошибки в Telegram. Парсер: {parser_name}, Топик: {topic_id}")
        
        # Форматируем сообщение об ошибке
        message = "⚠️ <b>ОШИБКА ПАРСЕРА</b>\n\n"
        if parser_name:
            message += f"📌 <b>Парсер:</b> {parser_name}\n"
        message += f"❌ <b>Ошибка:</b> {error_message}\n"
        message += f"⏰ <b>Время:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        
        # Retry логика для отправки ошибки
        for attempt in range(max_retries):
            try:
                if topic_id:
                    self.logger.debug(f"Отправка ошибки в топик {topic_id} (попытка {attempt + 1}/{max_retries})")
                    await self.bot.send_message(
                        chat_id=self.group_id,
                        text=message,
                        parse_mode='HTML',
                        message_thread_id=topic_id
                    )
                else:
                    self.logger.warning("ID топика Errors не найден, отправка без топика")
                    await self.bot.send_message(
                        chat_id=self.group_id,
                        text=message,
                        parse_mode='HTML'
                    )
                
                # Успешная отправка
                self.logger.info(f"Ошибка успешно отправлена в Telegram (попытка {attempt + 1})")
                return
                
            except asyncio.TimeoutError:
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 2
                    self.logger.warning(
                        f"Таймаут отправки ошибки в Telegram, повтор через {wait_time}с (попытка {attempt + 1}/{max_retries})"
                    )
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    self.logger.error(f"Таймаут отправки ошибки в Telegram после {max_retries} попыток")
                    return
                    
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 1.5
                    self.logger.warning(
                        f"Ошибка отправки ошибки в Telegram: {e}, повтор через {wait_time}с (попытка {attempt + 1}/{max_retries})"
                    )
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    self.logger.error(f"Ошибка отправки ошибки в Telegram после {max_retries} попыток: {e}")
                    return

# ============================================================================
# МОНИТОРИНГ С УЛУЧШЕНИЯМИ
# ============================================================================

class CommentMonitor:
    """Улучшенный класс для мониторинга комментариев"""
    
    def __init__(self):
        # Настройка логирования ПЕРВОЙ
        logging.basicConfig(
            level=getattr(logging, LOG_LEVEL),
            format=LOG_FORMAT,
            handlers=[
                logging.FileHandler(LOG_FILE, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        
        self.logger = logging.getLogger("monitor")
        
        self.parsers = self.get_configured_parsers()
        self.last_comments = {}
        self.check_interval = CHECK_INTERVAL
        self.telegram_sender = TelegramSender()
        self.state_file = "monitor_state_improved.json"
        self.first_run = True
        
        # Метрики
        self.stats = {
            'total_checks': 0,
            'total_comments_found': 0,
            'total_comments_sent': 0,
            'total_errors': 0,
            'parser_stats': defaultdict(lambda: {
                'checks': 0,
                'comments_found': 0,
                'errors': 0,
                'last_check': None
            }),
            'start_time': datetime.now()
        }
        
        # Graceful shutdown
        self.shutdown_event = asyncio.Event()
        
        # Время запуска парсера - комментарии до этого времени не отправляются
        # ВСЕГДА устанавливаем текущее время при запуске (не загружаем из файла)
        # Используем UTC для единообразия с комментариями из API
        from datetime import timezone as tz
        self.parser_start_time = datetime.now(tz.utc).replace(tzinfo=None)
        self.logger.info(f"Запуск парсера. Время запуска (UTC): {self.parser_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"Комментарии, написанные до {self.parser_start_time.strftime('%Y-%m-%d %H:%M:%S')} UTC, не будут отправляться")
        
        # Загружаем состояние (комментарии, но не время запуска)
        self.load_state()
        
        # Определяем, первый ли это запуск (если нет сохраненных комментариев)
        if not self.last_comments:
            self.first_run = True
            self.logger.info("Первый запуск: нет сохраненных комментариев")
        else:
            self.first_run = False
            self.logger.info(f"Загружено состояние для {len(self.last_comments)} парсеров")
    
    def get_configured_parsers(self) -> List:
        """Возвращает список настроенных парсеров"""
        parsers = []
        
        if SOCIAL_NETWORKS['youtube']['enabled'] and SOCIAL_NETWORKS['youtube']['api_key'] and SOCIAL_NETWORKS['youtube']['channel_id']:
            youtube_parser = create_youtube_parser(
                SOCIAL_NETWORKS['youtube']['api_key'],
                SOCIAL_NETWORKS['youtube']['channel_id']
            )
            parsers.append(youtube_parser)
            self.logger.info("YouTube парсер добавлен")
        
        if SOCIAL_NETWORKS['vk']['enabled'] and SOCIAL_NETWORKS['vk']['access_token'] and SOCIAL_NETWORKS['vk']['group_id']:
            vk_parser = create_vk_parser(
                SOCIAL_NETWORKS['vk']['access_token'],
                SOCIAL_NETWORKS['vk']['group_id'],
                SOCIAL_NETWORKS['vk']['group_url']
            )
            parsers.append(vk_parser)
            self.logger.info("VK парсер добавлен")
        
        if SOCIAL_NETWORKS['reddit']['enabled'] and SOCIAL_NETWORKS['reddit']['client_id'] and SOCIAL_NETWORKS['reddit']['client_secret']:
            for subreddit in SOCIAL_NETWORKS['reddit']['subreddits']:
                subreddit = subreddit.strip()
                if subreddit:
                    reddit_parser = create_reddit_parser(
                        SOCIAL_NETWORKS['reddit']['client_id'],
                        SOCIAL_NETWORKS['reddit']['client_secret'],
                        SOCIAL_NETWORKS['reddit']['user_agent'],
                        subreddit
                    )
                    parsers.append(reddit_parser)
                    self.logger.info(f"Reddit парсер для r/{subreddit} добавлен")
        
        return parsers
    
    def save_last_comments(self, parser_name: str, comments: List[Comment]):
        """Сохраняет последние комментарии для парсера"""
        # Сохраняем все комментарии для дедупликации (включая старые)
        # Фильтрация по времени запуска происходит в get_new_comments
        self.last_comments[parser_name] = comments[:100]
    
    def get_new_comments(self, parser_name: str, current_comments: List[Comment]) -> List[Comment]:
        """Определяет новые комментарии, фильтруя те, что были написаны до запуска парсера"""
        # Сначала фильтруем комментарии по времени запуска парсера
        filtered_comments = []
        skipped_before_start = 0
        for comment in current_comments:
            # Отправляем только комментарии, написанные после запуска парсера
            # Приводим timestamp к naive datetime для корректного сравнения
            try:
                comment_time = comment.timestamp
                if comment_time.tzinfo is not None:
                    # Если timezone-aware (например, из YouTube API), конвертируем в naive
                    # Используем UTC для преобразования
                    from datetime import timezone as tz
                    comment_time = comment_time.astimezone(tz.utc).replace(tzinfo=None)
                
                if comment_time >= self.parser_start_time:
                    filtered_comments.append(comment)
                else:
                    skipped_before_start += 1
                    # Для YouTube логируем комментарии, которые близки к времени запуска (в пределах 5 минут)
                    time_diff = (self.parser_start_time - comment_time).total_seconds()
                    if parser_name == "YouTube" and time_diff < 300:  # 5 минут
                        self.logger.info(f"{parser_name}: пропущен комментарий от {comment.author} (время: {comment_time.strftime('%Y-%m-%d %H:%M:%S')}, запуск парсера: {self.parser_start_time.strftime('%Y-%m-%d %H:%M:%S')}, разница: {time_diff:.0f}с)")
                    else:
                        self.logger.debug(f"{parser_name}: пропущен комментарий от {comment.author} (время: {comment_time.strftime('%Y-%m-%d %H:%M:%S')}, запуск парсера: {self.parser_start_time.strftime('%Y-%m-%d %H:%M:%S')})")
            except Exception as e:
                # Если ошибка при сравнении, пропускаем комментарий
                self.logger.warning(f"{parser_name}: ошибка сравнения времени комментария: {e}, пропускаем комментарий")
                skipped_before_start += 1
        
        if skipped_before_start > 0:
            self.logger.info(f"{parser_name}: пропущено {skipped_before_start} комментариев, написанных до запуска парсера ({self.parser_start_time.strftime('%Y-%m-%d %H:%M:%S')})")
        
        if self.first_run:
            self.logger.info(f"Первый запуск: пропускаем отправку старых комментариев для {parser_name}")
            return []
        
        # Если парсера нет в last_comments, считаем что это первый запуск для этого парсера
        # и не отправляем комментарии (чтобы избежать дублирования)
        if parser_name not in self.last_comments:
            self.logger.info(f"{parser_name}: первый запуск парсера, пропускаем комментарии")
            return []
        
        last_comments = self.last_comments[parser_name]
        
        known_comments = set()
        for comment in last_comments:
            # Для всех парсеров используем одинаковую логику: author + text + source_url
            # source_url содержит уникальный ID комментария
            key = f"{comment.author}_{comment.text}_{comment.source_url}"
            known_comments.add(key)
        
        self.logger.debug(f"{parser_name}: в базе {len(known_comments)} известных комментариев")
        
        new_comments = []
        for comment in filtered_comments:
            # Для всех парсеров используем одинаковую логику: author + text + source_url
            key = f"{comment.author}_{comment.text}_{comment.source_url}"
            
            if key not in known_comments:
                new_comments.append(comment)
            else:
                self.logger.debug(f"{parser_name}: пропущен уже обработанный комментарий от {comment.author}")
        
        self.logger.info(f"{parser_name}: найдено {len(current_comments)} комментариев ({len(filtered_comments)} после запуска), {len(new_comments)} новых (в базе: {len(known_comments)})")
        return new_comments
    
    def format_report(self, parser_name, new_comments):
        """Форматирует отчет о новых комментариях"""
        if not new_comments:
            return None
        
        if parser_name.lower().startswith("reddit"):
            # Извлекаем сабреддит из parser_name, например "Reddit (r/python)" -> "python"
            import re
            match = re.search(r'r/(\w+)', parser_name)
            if match:
                subreddit = match.group(1)
                report = f"\n🆕 НОВЫЕ КОММЕНТАРИИ из REDDIT (r/{subreddit})\n"
            else:
                report = f"\n🆕 НОВЫЕ КОММЕНТАРИИ из {parser_name.upper()}\n"
        else:
            report = f"\n🆕 НОВЫЕ КОММЕНТАРИИ из {parser_name.upper()}\n"
        
        report += "=" * 50 + "\n"
        
        for i, comment in enumerate(new_comments, 1):
            text = comment.text.replace('\n', ' ').replace('<br>', ' ')[:60]
            report += f"{i}. {comment.author}: {text}...\n"
            report += f"   🔗 {comment.source_url}\n"
            report += f"   ⏰ {comment.timestamp.strftime('%H:%M:%S')}\n\n"
        
        return report
    
    async def _check_single_parser(self, parser) -> Dict:
        """Проверяет один парсер (для параллельного выполнения)"""
        parser_name = parser.source_name
        result = {
            'parser_name': parser_name,
            'comments': [],
            'new_comments': [],
            'error': None
        }
        
        try:
            self.stats['parser_stats'][parser_name]['checks'] += 1
            self.stats['parser_stats'][parser_name]['last_check'] = datetime.now()
            
            # Для Reddit: до 20 комментариев с каждого из 20 постов = до 400 комментариев
            # Для других: до 30 комментариев с каждого из 20 постов/видео = до 600 комментариев
            per_parser_limit = 400 if parser_name.startswith("Reddit") else 600
            comments = await parser.get_comments(limit=per_parser_limit)
            
            if comments:
                self.stats['parser_stats'][parser_name]['comments_found'] += len(comments)
                new_comments = self.get_new_comments(parser_name, comments)
                
                result['comments'] = comments
                result['new_comments'] = new_comments
                
                if new_comments:
                    limited_comments = new_comments[:10]
                    if len(new_comments) > 10:
                        self.logger.info(f"{parser_name}: ограничено до 10 комментариев из {len(new_comments)}")
                    
                    # Отправляем комментарии
                    for comment in limited_comments:
                        await self.telegram_sender.send_comment(comment, parser_name)
                        self.stats['total_comments_sent'] += 1
                    
                    report = self.format_report(parser_name, new_comments)
                    if report:
                        print(report)
                        self.logger.info(f"{parser_name}: найдено {len(new_comments)} новых комментариев")
                
                self.save_last_comments(parser_name, comments)
            else:
                self.logger.info(f"{parser_name}: комментариев не найдено")
                self.save_last_comments(parser_name, [])
                
        except YouTubeQuotaExceeded as e:
            # Специальная обработка ошибки квоты YouTube API
            error_msg = str(e)
            self.logger.error(f"ПЕРЕХВАЧЕНА ОШИБКА КВОТЫ YOUTUBE: {error_msg}")
            self.stats['total_errors'] += 1
            self.stats['parser_stats'][parser_name]['errors'] += 1
            result['error'] = error_msg
            
            # Отправляем ошибку в топик ошибок Telegram
            self.logger.info(f"Вызываю send_error для парсера {parser_name}")
            try:
                await self.telegram_sender.send_error(error_msg, parser_name=parser_name)
                self.logger.info(f"send_error завершен для парсера {parser_name}")
            except Exception as send_err:
                self.logger.error(f"ОШИБКА при вызове send_error: {send_err}")
            
            # Выводим сообщение в консоль
            print(f"\n⚠️ {parser_name}: {error_msg}\n")
            
        except Exception as e:
            error_msg = f"Ошибка при проверке {parser_name}: {e}"
            self.logger.error(error_msg)
            self.stats['total_errors'] += 1
            self.stats['parser_stats'][parser_name]['errors'] += 1
            result['error'] = str(e)
            
            # Отправляем ошибку в топик ошибок Telegram
            await self.telegram_sender.send_error(error_msg, parser_name=parser_name)
        
        return result
    
    async def check_parsers(self):
        """Проверяет все парсеры ПАРАЛЛЕЛЬНО"""
        self.logger.info("Проверяю парсеры...")
        self.stats['total_checks'] += 1
        
        if not self.parsers:
            self.logger.warning("Нет активных парсеров для проверки")
            return
        
        # Создаем задачи для параллельного выполнения
        tasks = [self._check_single_parser(parser) for parser in self.parsers]
        
        # Выполняем все парсеры параллельно
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Обрабатываем результаты
        for result in results:
            if isinstance(result, Exception):
                # Специальная обработка для YouTubeQuotaExceeded
                if isinstance(result, YouTubeQuotaExceeded):
                    error_msg = str(result)
                    self.logger.error(f"Критическая ошибка парсера (квота YouTube): {error_msg}")
                    self.stats['total_errors'] += 1
                    # Отправляем ошибку в Telegram
                    await self.telegram_sender.send_error(error_msg, parser_name="YouTube")
                else:
                    error_msg = f"Критическая ошибка парсера: {result}"
                    self.logger.error(error_msg)
                    self.stats['total_errors'] += 1
                    # Отправляем ошибку в Telegram
                    await self.telegram_sender.send_error(str(result), parser_name="Unknown")
                continue
            
            parser_name = result['parser_name']
            if result['comments']:
                self.stats['total_comments_found'] += len(result['comments'])
        
        # Сохраняем состояние ОДИН раз после всех проверок
        self.save_state()
        
        # Выводим статистику каждые 10 проверок
        if self.stats['total_checks'] % 10 == 0:
            self.print_stats()
    
    def print_stats(self):
        """Выводит статистику работы"""
        uptime = datetime.now() - self.stats['start_time']
        self.logger.info("=" * 50)
        self.logger.info("СТАТИСТИКА МОНИТОРИНГА")
        self.logger.info(f"Время работы: {uptime}")
        self.logger.info(f"Всего проверок: {self.stats['total_checks']}")
        self.logger.info(f"Всего комментариев найдено: {self.stats['total_comments_found']}")
        self.logger.info(f"Всего комментариев отправлено: {self.stats['total_comments_sent']}")
        self.logger.info(f"Всего ошибок: {self.stats['total_errors']}")
        self.logger.info("-" * 50)
        for parser_name, stats in self.stats['parser_stats'].items():
            self.logger.info(f"{parser_name}: проверок={stats['checks']}, найдено={stats['comments_found']}, ошибок={stats['errors']}")
        self.logger.info("=" * 50)
    
    async def run(self):
        """Запускает мониторинг с graceful shutdown"""
        self.logger.info("Запуск мониторинга комментариев")
        self.logger.info("Для остановки нажмите Ctrl+C")
        
        # Инициализируем Telegram бота
        self.logger.info("Подключение к Telegram боту...")
        if not await self.telegram_sender.setup_bot():
            self.logger.error("❌ Не удалось подключиться к Telegram боту!")
            self.logger.error("Мониторинг не будет запущен без Telegram бота")
            return
        
        self.logger.info("Telegram бот подключен успешно")
        self.logger.info(f"Запуск мониторинга {len(self.parsers)} парсеров...")
        
        # Устанавливаем обработчики сигналов для graceful shutdown
        try:
            if platform.system() != 'Windows':
                loop = asyncio.get_event_loop()
                for sig in (signal.SIGTERM, signal.SIGINT):
                    loop.add_signal_handler(sig, lambda: self.shutdown_event.set())
        except Exception as e:
            self.logger.warning(f"Не удалось установить обработчики сигналов: {e}")
        
        try:
            while not self.shutdown_event.is_set():
                await self.check_parsers()
                
                if self.first_run:
                    self.first_run = False
                    self.logger.info("Первый запуск завершен, теперь будут отправляться только новые комментарии")
                
                self.logger.info(f"Ожидание {self.check_interval} секунд...")
                
                # Проверяем shutdown_event во время ожидания
                try:
                    await asyncio.wait_for(
                        self.shutdown_event.wait(),
                        timeout=self.check_interval
                    )
                    # Если событие установлено, выходим из цикла
                    break
                except asyncio.TimeoutError:
                    pass  # Таймаут ожидания истек, продолжаем цикл
                except asyncio.CancelledError:
                    self.logger.info("Получен сигнал отмены")
                    break
                
        except KeyboardInterrupt:
            self.logger.info("Получен сигнал остановки (Ctrl+C)")
        except Exception as e:
            self.logger.error(f"Критическая ошибка: {e}")
        finally:
            self.logger.info("Завершение работы мониторинга...")
            
            # Закрываем сессии парсеров
            for parser in self.parsers:
                try:
                    await parser.close_session()
                except Exception as e:
                    self.logger.warning(f"Ошибка закрытия сессии парсера {parser.source_name}: {e}")
            
            # Сохраняем состояние
            self.save_state()
            self.print_stats()
            self.logger.info("Мониторинг остановлен корректно")
    
    def load_state(self):
        """Загружает состояние из файла"""
        try:
            if os.path.exists(self.state_file):
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    restored_comments = {}
                    for source, comments_data in data.get('last_comments', {}).items():
                        comments = []
                        for comment_data in comments_data:
                            comment = Comment(
                                author=comment_data['author'],
                                text=comment_data['text'],
                                source=source,
                                timestamp=datetime.fromisoformat(comment_data['timestamp']),
                                source_url=comment_data['source_url']
                            )
                            comments.append(comment)
                        restored_comments[source] = comments
                    self.last_comments = restored_comments
                    
                    # НЕ загружаем время запуска из файла - оно всегда устанавливается в текущее время при запуске
                    # Время запуска в файле используется только для информации
                    if 'parser_start_time' in data:
                        saved_time = datetime.fromisoformat(data['parser_start_time'])
                        if saved_time.tzinfo is not None:
                            saved_time = saved_time.replace(tzinfo=None)
                        self.logger.debug(f"В файле сохранено время предыдущего запуска: {saved_time.strftime('%Y-%m-%d %H:%M:%S')} (не используется)")
                    
                    self.logger.info("Состояние загружено из файла")
            else:
                # Файл состояния не существует - будет установлено в __init__
                self.logger.info("Файл состояния не найден, будет создан новый")
        except Exception as e:
            self.logger.error(f"Ошибка загрузки состояния: {e}")
            # При ошибке загрузки оставляем None, будет установлено в __init__
    
    def save_state(self):
        """Сохраняет состояние в файл"""
        try:
            json_comments = {}
            for source, comments in self.last_comments.items():
                json_comments[source] = []
                for comment in comments:
                    json_comments[source].append({
                        'author': comment.author,
                        'text': comment.text,
                        'source': comment.source,
                        'timestamp': comment.timestamp.isoformat(),
                        'source_url': comment.source_url
                    })
            
            data = {
                'last_comments': json_comments,
                'timestamp': datetime.now().isoformat(),
                'parser_start_time': self.parser_start_time.isoformat() if self.parser_start_time else datetime.now().isoformat(),  # Сохраняем время запуска
                'stats': {
                    'total_checks': self.stats['total_checks'],
                    'total_comments_found': self.stats['total_comments_found'],
                    'total_comments_sent': self.stats['total_comments_sent']
                }
            }
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.logger.error(f"Ошибка сохранения состояния: {e}")

# ============================================================================
# ГЛАВНАЯ ФУНКЦИЯ
# ============================================================================

def main():
    """Главная функция"""
    print(f"🔧 Окружение: {ENVIRONMENT}")
    print(f"⏰ Интервал проверки: {CHECK_INTERVAL} секунд")
    print(f"📝 Уровень логирования: {LOG_LEVEL}")
    
    if ENVIRONMENT == 'local':
        print("🏠 ЛОКАЛЬНЫЙ РЕЖИМ")
        print("⏰ Интервал: 30 секунд")
    else:
        print("🖥️ СЕРВЕРНЫЙ РЕЖИМ")
        print("⏰ Интервал: 30 секунд")
    
    print("=" * 40)
    print("🔍 МОНИТОРИНГ КОММЕНТАРИЕВ")
    print("=" * 40)
    
    # Проверяем конфигурацию
    print("🔍 Проверка конфигурации...")
    
    # Проверяем Telegram
    if not TELEGRAM_BOT_TOKEN or TELEGRAM_BOT_TOKEN == 'YOUR_BOT_TOKEN_HERE':
        print("❌ TELEGRAM_BOT_TOKEN не настроен!")
        return
    if not TELEGRAM_GROUP_ID or TELEGRAM_GROUP_ID == 'YOUR_GROUP_ID_HERE':
        print("❌ TELEGRAM_GROUP_ID не настроен!")
        return
    
    # Проверяем YouTube
    if not YOUTUBE_API_KEY or YOUTUBE_API_KEY == 'YOUR_YOUTUBE_API_KEY_HERE':
        print("WARNING: YouTube парсер отключен (нет API ключа)")
    else:
        print("OK: YouTube парсер настроен")
    
    # Проверяем VK
    if not VK_ACCESS_TOKEN or VK_ACCESS_TOKEN == 'YOUR_VK_ACCESS_TOKEN_HERE':
        print("WARNING: VK парсер отключен (нет токена)")
    else:
        print("OK: VK парсер настроен")
    
    # Проверяем Reddit
    if not REDDIT_CLIENT_ID or REDDIT_CLIENT_ID == 'YOUR_REDDIT_CLIENT_ID_HERE':
        print("WARNING: Reddit парсер отключен (нет настроек)")
    else:
        print("OK: Reddit парсер настроен")
    
    print("=" * 40)
    
    # Создаем и запускаем монитор
    monitor = CommentMonitor()
    monitor.logger.info(f"Инициализирован мониторинг {len(monitor.parsers)} парсеров")
    monitor.logger.info(f"Интервал проверки: {monitor.check_interval} секунд")
    
    # Запускаем мониторинг
    asyncio.run(monitor.run())

if __name__ == "__main__":
    main()


#!/usr/bin/env python3
"""
Улучшенный VK парсер с connection pooling, retry логикой и оптимизациями
"""

import asyncio
import aiohttp
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from abc import ABC, abstractmethod
import ssl

class Comment:
    """Класс для представления комментария"""
    def __init__(self, author: str, text: str, source: str, timestamp: datetime, source_url: str = ""):
        self.author = author
        self.text = text
        self.source = source
        self.timestamp = timestamp
        self.source_url = source_url
    
    def __str__(self):
        return f"[{self.source}] {self.author}: {self.text[:50]}..."

class BaseParser(ABC):
    """Базовый класс для всех парсеров социальных сетей"""
    
    def __init__(self, source_name: str):
        self.source_name = source_name
        self.logger = logging.getLogger(f"parser.{source_name}")
        self.last_check_time = None
        self.processed_comments = set()
        self._session = None  # Переиспользуемая сессия
    
    @abstractmethod
    async def get_comments(self, limit: int = 50) -> List[Comment]:
        """Получает комментарии из социальной сети"""
        pass
    
    @abstractmethod
    def is_configured(self) -> bool:
        """Проверяет, настроен ли парсер"""
        pass
    
    async def _get_session(self) -> aiohttp.ClientSession:
        """Получает или создает переиспользуемую HTTP сессию"""
        if self._session is None or self._session.closed:
            # Создаем SSL контекст для обхода проблем с сертификатами
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            
            connector = aiohttp.TCPConnector(
                ssl=ssl_context,
                limit=100,  # Максимум соединений
                limit_per_host=30,  # Максимум на один хост
                ttl_dns_cache=300  # Кэш DNS на 5 минут
            )
            timeout = aiohttp.ClientTimeout(total=30, connect=10)
            self._session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout
            )
        return self._session
    
    async def close_session(self):
        """Закрывает HTTP сессию"""
        if self._session and not self._session.closed:
            await self._session.close()
    
    async def make_request_with_retry(
        self,
        method: str,
        url: str,
        max_retries: int = 3,
        **kwargs
    ) -> Optional[Dict]:
        """Выполняет HTTP запрос с повторными попытками и возвращает JSON"""
        session = await self._get_session()
        
        for attempt in range(max_retries):
            try:
                async with session.request(method, url, **kwargs) as response:
                    if response.status == 200:
                        try:
                            data = await response.json()
                            return data
                        except Exception as e:
                            self.logger.error(f"Ошибка парсинга JSON ответа: {e}")
                            if attempt < max_retries - 1:
                                wait_time = 2 ** attempt
                                await asyncio.sleep(wait_time)
                                continue
                            return None
                    elif response.status in [429, 500, 502, 503, 504]:  # Временные ошибки
                        if attempt < max_retries - 1:
                            wait_time = 2 ** attempt  # Exponential backoff
                            self.logger.warning(
                                f"Получен статус {response.status}, повтор через {wait_time}с (попытка {attempt + 1}/{max_retries})"
                            )
                            await asyncio.sleep(wait_time)
                            continue
                    else:
                        self.logger.error(f"HTTP ошибка {response.status} при запросе к {url}")
                        return None
            except asyncio.TimeoutError:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    self.logger.warning(f"Таймаут запроса, повтор через {wait_time}с (попытка {attempt + 1}/{max_retries})")
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    self.logger.error(f"Таймаут запроса к {url} после {max_retries} попыток")
                    return None
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    self.logger.warning(f"Ошибка запроса: {e}, повтор через {wait_time}с (попытка {attempt + 1}/{max_retries})")
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    self.logger.error(f"Ошибка запроса к {url}: {e}")
                    return None
        
        return None
    
    def get_unique_id(self, comment: Comment) -> str:
        """Создает уникальный ID для комментария"""
        return f"{comment.source}_{comment.author}_{comment.timestamp}_{hash(comment.text)}"
    
    def is_new_comment(self, comment: Comment) -> bool:
        """Проверяет, является ли комментарий новым"""
        comment_id = self.get_unique_id(comment)
        if comment_id in self.processed_comments:
            return False
        
        self.processed_comments.add(comment_id)
        return True

class VKParser(BaseParser):
    """Улучшенный парсер комментариев VK через API"""
    
    def __init__(self, access_token: str, group_id: str, group_url: str = ""):
        super().__init__("VK")
        self.access_token = access_token
        self.group_id = group_id
        self.group_url = group_url
        self.api_version = "5.131"
        self.base_url = "https://api.vk.com/method"
        self.profiles = {}  # Кэш профилей пользователей
        
    def is_configured(self) -> bool:
        """Проверяет, настроен ли парсер VK"""
        return bool(self.access_token and self.group_id)
    
    async def make_api_request(self, method: str, params: Dict[str, Any]) -> Optional[Dict]:
        """Выполняет запрос к VK API с retry логикой"""
        url = f"{self.base_url}/{method}"
        params.update({
            'access_token': self.access_token,
            'v': self.api_version
        })
        
        data = await self.make_request_with_retry('GET', url, params=params)
        if data:
            if 'error' in data:
                self.logger.error(f"VK API ошибка: {data['error']}")
                return None
            return data.get('response')
        return None
    
    async def get_group_posts(self, count: int = 20) -> List[Dict]:
        """Получает последние посты группы"""
        if not self.is_configured():
            self.logger.error("VK парсер не настроен")
            return []
        
        group_id = self.group_id
        if not group_id.startswith('-'):
            group_id = f"-{group_id}"
        
        params = {
            'owner_id': group_id,
            'count': count,
            'filter': 'owner',
            'extended': 0
        }
        
        response = await self.make_api_request('wall.get', params)
        if response and 'items' in response:
            return response['items']
        return []
    
    async def get_post_comments(self, post_id: str, count: int = 20) -> List[Dict]:
        """Получает комментарии к конкретному посту"""
        if not self.is_configured():
            return []
        
        group_id = self.group_id
        if not group_id.startswith('-'):
            group_id = f"-{group_id}"
        
        params = {
            'owner_id': group_id,
            'post_id': post_id,
            'count': count,
            'sort': 'desc',
            'extended': 1,
            'fields': 'id,first_name,last_name,screen_name',
            'thread_items_count': 10
        }
        
        response = await self.make_api_request('wall.getComments', params)
        if response and 'items' in response:
            if 'profiles' in response:
                self.profiles = {p['id']: p for p in response['profiles']}
            return response['items']
        return []
    
    def parse_comment(self, comment_data: Dict, profiles: Dict = None, post_id: str = None) -> Comment:
        """Парсит данные комментария в объект Comment"""
        author_name = "Неизвестный"
        if profiles and comment_data.get('from_id') in profiles:
            profile = profiles[comment_data['from_id']]
            first_name = profile.get('first_name', '')
            last_name = profile.get('last_name', '')
            author_name = f"{first_name} {last_name}".strip()
        elif comment_data.get('from_id'):
            author_name = f"ID{comment_data['from_id']}"
        
        text = comment_data.get('text', '').strip()
        timestamp = datetime.fromtimestamp(comment_data.get('date', 0))
        
        comment_id = comment_data.get('id', '')
        if post_id and self.group_url:
            source_url = f"{self.group_url}?reply={comment_id}&w=wall-{self.group_id}_{post_id}"
        else:
            source_url = f"{self.group_url}?reply={comment_id}" if self.group_url else ""
        
        return Comment(
            author=author_name,
            text=text,
            source=self.source_name,
            timestamp=timestamp,
            source_url=source_url
        )
    
    async def get_comments(self, limit: int = 50) -> List[Comment]:
        """Получает комментарии из VK с оптимизацией"""
        if not self.is_configured():
            self.logger.error("VK парсер не настроен")
            return []
        
        all_comments = []
        
        try:
            # Получаем последние посты группы
            posts = await self.get_group_posts(count=20)
            self.logger.info(f"VK: найдено {len(posts)} постов")
            
            if not posts:
                return []
            
            # Параллельно получаем комментарии ко всем постам
            tasks = []
            for post in posts:
                post_id = post.get('id')
                if post_id:
                    task = self.get_post_comments(str(post_id), count=30)
                    tasks.append((post_id, task))
            
            # Выполняем все запросы параллельно
            results = await asyncio.gather(*[task for _, task in tasks], return_exceptions=True)
            
            # Обрабатываем результаты
            for i, (post_id, _) in enumerate(tasks):
                if isinstance(results[i], Exception):
                    self.logger.error(f"Ошибка при получении комментариев к посту {post_id}: {results[i]}")
                    continue
                
                comments_data = results[i]
                if comments_data:
                    for comment_data in comments_data:
                        comment = self.parse_comment(comment_data, self.profiles, post_id)
                        if comment.text:
                            all_comments.append(comment)
                        
                        thread = comment_data.get('thread', {})
                        if thread and 'items' in thread:
                            for reply_data in thread['items']:
                                reply = self.parse_comment(reply_data, self.profiles, post_id)
                                if reply.text:
                                    reply.text = f"↳ {reply.text}"
                                    all_comments.append(reply)
            
            # Сортируем по времени (новые сначала)
            all_comments.sort(key=lambda x: x.timestamp, reverse=True)
            
            result = all_comments[:limit]
            self.logger.info(f"VK: получено {len(result)} комментариев")
            return result
            
        except Exception as e:
            self.logger.error(f"Ошибка при получении комментариев VK: {e}")
            return []

# Функция для создания VK парсера
def create_vk_parser(access_token: str, group_id: str, group_url: str = "") -> VKParser:
    """Создает и возвращает улучшенный VK парсер"""
    return VKParser(access_token, group_id, group_url)


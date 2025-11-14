#!/usr/bin/env python3
"""
Улучшенный Reddit парсер с кэшированием токенов и оптимизациями
"""

import asyncio
import aiohttp
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from abc import ABC, abstractmethod
import ssl
import random

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
        self._session = None
    
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
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            
            connector = aiohttp.TCPConnector(
                ssl=ssl_context,
                limit=100,
                limit_per_host=30,
                ttl_dns_cache=300
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
                    elif response.status in [429, 500, 502, 503, 504]:
                        if attempt < max_retries - 1:
                            wait_time = 2 ** attempt
                            self.logger.warning(
                                f"Статус {response.status}, повтор через {wait_time}с (попытка {attempt + 1}/{max_retries})"
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

class RedditParser(BaseParser):
    """Улучшенный парсер комментариев Reddit через API с кэшированием токенов"""
    
    def __init__(self, client_id: str, client_secret: str, user_agent: str, subreddit: str):
        super().__init__(f"Reddit (r/{subreddit})")
        self.client_id = client_id
        self.client_secret = client_secret
        self.user_agent = user_agent
        self.subreddit = subreddit
        self.access_token = None
        self.token_expires_at = None  # Время истечения токена
        self.base_url = "https://oauth.reddit.com"
        self.auth_url = "https://www.reddit.com/api/v1/access_token"
    
    def is_configured(self) -> bool:
        """Проверяет, настроен ли парсер Reddit"""
        return bool(self.client_id and self.client_secret and self.user_agent and self.subreddit)
    
    async def get_access_token(self) -> bool:
        """Получает access token для Reddit API с кэшированием"""
        # Проверяем, не истек ли токен
        if self.access_token and self.token_expires_at:
            if datetime.now() < self.token_expires_at:
                self.logger.debug("Используется существующий Reddit токен")
                return True  # Токен еще валиден
        
        if not self.is_configured():
            return False
        
        auth = aiohttp.BasicAuth(self.client_id, self.client_secret)
        headers = {
            'User-Agent': self.user_agent
        }
        data = {
            'grant_type': 'client_credentials'
        }
        
        # Для получения токена используем отдельную сессию без SSL проблем
        # как в старой версии, это важно для Reddit API
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(self.auth_url, auth=auth, headers=headers, data=data) as response:
                    if response.status == 200:
                        token_data = await response.json()
                        self.access_token = token_data.get('access_token')
                        expires_in = token_data.get('expires_in', 3600)
                        # Сохраняем время истечения с запасом в 60 секунд
                        self.token_expires_at = datetime.now() + timedelta(seconds=expires_in - 60)
                        self.logger.info("Reddit access token получен и закэширован")
                        return True
                    else:
                        self.logger.error(f"Ошибка получения Reddit token: {response.status}")
                        return False
        except Exception as e:
            self.logger.error(f"Ошибка при получении Reddit token: {e}")
            return False
    
    async def make_api_request(self, endpoint: str, params: Dict[str, Any] = None, max_retries: int = 3) -> Optional[Dict]:
        """Выполняет запрос к Reddit API с retry логикой для таймаутов"""
        if not await self.get_access_token():
            self.logger.error(f"Не удалось получить токен для запроса {endpoint}")
            return None
        
        url = f"{self.base_url}{endpoint}"
        headers = {
            'Authorization': f'bearer {self.access_token}',
            'User-Agent': self.user_agent
        }
        
        # Retry логика для таймаутов
        for attempt in range(max_retries):
            try:
                # Используем отдельную сессию для каждого запроса, как в старой версии
                import ssl
                ssl_context = ssl.create_default_context()
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
                
                connector = aiohttp.TCPConnector(ssl=ssl_context)
                # Увеличенный таймаут для Reddit API
                timeout = aiohttp.ClientTimeout(total=45, connect=15)
                
                async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                    async with session.get(url, headers=headers, params=params) as response:
                        if response.status == 200:
                            data = await response.json()
                            return data
                        elif response.status == 401:
                            # Token expired, try to get new one
                            self.logger.warning(f"Получен 401 для {endpoint}, обновляем токен")
                            self.access_token = None
                            self.token_expires_at = None
                            if await self.get_access_token():
                                headers['Authorization'] = f'bearer {self.access_token}'
                                async with session.get(url, headers=headers, params=params) as retry_response:
                                    if retry_response.status == 200:
                                        return await retry_response.json()
                        elif response.status == 429:
                            # Rate limit - ждем дольше
                            if attempt < max_retries - 1:
                                wait_time = (attempt + 1) * 5  # 5, 10, 15 секунд
                                self.logger.warning(f"Rate limit (429) для {endpoint}, ждем {wait_time}с (попытка {attempt + 1}/{max_retries})")
                                await asyncio.sleep(wait_time)
                                continue
                        else:
                            self.logger.error(f"Reddit API ошибка для {endpoint}: статус {response.status}")
                            return None
            except asyncio.TimeoutError:
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 3  # 3, 6, 9 секунд
                    self.logger.warning(f"Таймаут запроса к {endpoint}, повтор через {wait_time}с (попытка {attempt + 1}/{max_retries})")
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    self.logger.error(f"Таймаут запроса к {endpoint} после {max_retries} попыток")
                    return None
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 2
                    self.logger.warning(f"Ошибка запроса к Reddit API ({endpoint}): {e}, повтор через {wait_time}с (попытка {attempt + 1}/{max_retries})")
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    self.logger.error(f"Ошибка запроса к Reddit API ({endpoint}): {e}")
                    return None
        
        return None
    
    async def get_subreddit_posts(self, limit: int = 20) -> List[Dict]:
        """Получает последние посты из сабреддита"""
        params = {
            'limit': limit,
            'sort': 'new'
        }
        
        response = await self.make_api_request(f'/r/{self.subreddit}/new', params)
        if response:
            if 'data' in response and 'children' in response['data']:
                posts = response['data']['children']
                self.logger.debug(f"Reddit: получено {len(posts)} постов из /r/{self.subreddit}/new")
                return posts
            else:
                self.logger.warning(f"Reddit: неожиданный формат ответа для /r/{self.subreddit}/new: {list(response.keys())}")
        else:
            self.logger.warning(f"Reddit: пустой ответ от API для /r/{self.subreddit}/new")
        return []
    
    async def get_post_comments(self, post_id: str, limit: int = 20) -> List[Dict]:
        """Получает комментарии к конкретному посту"""
        params = {
            'limit': limit,
            'sort': 'new'
        }
        
        response = await self.make_api_request(f'/r/{self.subreddit}/comments/{post_id}', params)
        if response and isinstance(response, list) and len(response) > 1:
            comments_data = response[1]['data']['children']
            return comments_data
        return []
    
    def parse_comment(self, comment_data: Dict) -> Comment:
        """Парсит данные комментария в объект Comment"""
        data = comment_data.get('data', {})
        
        author = data.get('author', 'Unknown')
        if author == '[deleted]':
            author = 'Deleted User'
        
        text = data.get('body', '').strip()
        
        created_utc = data.get('created_utc', 0)
        # Reddit возвращает timestamp в UTC, поэтому используем timezone.utc
        from datetime import timezone
        timestamp = datetime.fromtimestamp(created_utc, tz=timezone.utc).replace(tzinfo=None)
        
        comment_id = data.get('id', '')
        post_id = data.get('link_id', '').replace('t3_', '')
        source_url = f"https://reddit.com/r/{self.subreddit}/comments/{post_id}/_/{comment_id}/"
        
        return Comment(
            author=author,
            text=text,
            source=self.source_name,
            timestamp=timestamp,
            source_url=source_url
        )
    
    async def get_comments(self, limit: int = 50) -> List[Comment]:
        """Получает комментарии из Reddit с оптимизацией"""
        if not self.is_configured():
            self.logger.error("Reddit парсер не настроен")
            return []
        
        # Небольшая случайная задержка (0-2 секунды) чтобы избежать одновременных запросов
        # Это помогает избежать rate limiting когда несколько Reddit парсеров работают параллельно
        delay = random.uniform(0, 2)
        await asyncio.sleep(delay)
        
        all_comments = []
        
        try:
            # Получаем последние посты из сабреддита
            posts = await self.get_subreddit_posts(limit=20)
            self.logger.info(f"Reddit: найдено {len(posts)} постов")
            
            if not posts:
                return []
            
            # Параллельно получаем комментарии ко всем постам
            tasks = []
            for post in posts:
                post_data = post.get('data', {})
                post_id = post_data.get('id')
                if post_id:
                    tasks.append((post_id, self.get_post_comments(post_id, limit=20)))
            
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
                        if comment_data.get('data', {}).get('body') == '[deleted]':
                            continue
                        
                        comment = self.parse_comment(comment_data)
                        if comment.text:
                            all_comments.append(comment)
                        
                        replies = comment_data.get('data', {}).get('replies', {})
                        if replies and 'data' in replies and 'children' in replies['data']:
                            for reply_data in replies['data']['children']:
                                if reply_data.get('data', {}).get('body') != '[deleted]':
                                    reply = self.parse_comment(reply_data)
                                    if reply.text:
                                        reply.text = f"↳ {reply.text}"
                                        all_comments.append(reply)
            
            # Сортируем по времени (новые сначала)
            all_comments.sort(key=lambda x: x.timestamp, reverse=True)
            
            # Ограничиваем только по переданному limit
            # По требованиям: до 20 комментариев с каждого из 20 постов = до 400 комментариев с сабреддита
            # limit передается из main.py (20 для Reddit), но реально может быть больше комментариев
            # Поэтому используем limit как максимум, но не ограничиваем жестко до 20
            result = all_comments[:limit] if limit else all_comments
            self.logger.info(f"Reddit: получено {len(result)} комментариев из {len(all_comments)} найденных")
            return result
            
        except Exception as e:
            self.logger.error(f"Ошибка при получении комментариев Reddit: {e}")
            return []

# Функция для создания Reddit парсера
def create_reddit_parser(client_id: str, client_secret: str, user_agent: str, subreddit: str) -> RedditParser:
    """Создает и возвращает улучшенный Reddit парсер"""
    return RedditParser(client_id, client_secret, user_agent, subreddit)


#!/usr/bin/env python3
"""
Улучшенный YouTube парсер с connection pooling и оптимизациями
"""

import asyncio
import aiohttp
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from abc import ABC, abstractmethod
import ssl

class YouTubeQuotaExceeded(Exception):
    """Превышена дневная квота YouTube API"""
    pass

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
            connector = aiohttp.TCPConnector(
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
                    elif response.status == 403:
                        # Проверяем, не превышена ли квота YouTube API
                        try:
                            error_data = await response.json()
                            error_reason = error_data.get('error', {}).get('errors', [{}])[0].get('reason', '')
                            if error_reason == 'quotaExceeded':
                                error_msg = (
                                    "⚠️ YouTube API: превышена дневная квота (10,000 единиц). "
                                    "Квота обновится через 24 часа. YouTube парсер временно отключен."
                                )
                                self.logger.error(error_msg)
                                # Бросаем исключение, чтобы его можно было обработать в main.py
                                raise YouTubeQuotaExceeded(error_msg)
                        except YouTubeQuotaExceeded:
                            # Пробрасываем исключение дальше
                            raise
                        except:
                            # Для других ошибок парсинга просто продолжаем
                            pass
                        # Для других 403 ошибок просто логируем
                        self.logger.error(f"HTTP ошибка 403 при запросе к {url}")
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
            except YouTubeQuotaExceeded:
                # Пробрасываем исключение квоты дальше, не делаем retry
                raise
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

class YouTubeParser(BaseParser):
    """Улучшенный парсер комментариев YouTube через API"""
    
    def __init__(self, api_key: str, channel_id: str):
        super().__init__("YouTube")
        self.api_key = api_key
        self.channel_id = channel_id
        self.base_url = "https://www.googleapis.com/youtube/v3"
        self._uploads_playlist_id = None  # Кэш для uploads playlist ID
        self._playlist_id_loaded = False  # Флаг, что мы пытались загрузить playlist ID
    
    def is_configured(self) -> bool:
        """Проверяет, настроен ли парсер YouTube"""
        return bool(self.api_key and self.channel_id)
    
    async def get_channel_id_by_username(self, username: str) -> str:
        """Получает ID канала по username"""
        try:
            url = f"{self.base_url}/channels"
            params = {
                'part': 'id',
                'forUsername': username,
                'key': self.api_key
            }
            
            data = await self.make_request_with_retry('GET', url, params=params)
            if data and data.get('items'):
                return data['items'][0]['id']
            return None
        except YouTubeQuotaExceeded:
            # Пробрасываем исключение квоты дальше
            raise
        except Exception as e:
            self.logger.error(f"Ошибка при получении ID канала: {e}")
            return None
    
    async def get_uploads_playlist_id(self) -> Optional[str]:
        """Получает ID плейлиста 'Uploads' канала (кэшируется)"""
        if self._uploads_playlist_id is None and not self._playlist_id_loaded:
            self._playlist_id_loaded = True
            try:
                # Если channel_id это username, сначала получаем реальный ID
                channel_id_to_use = self.channel_id
                if not channel_id_to_use.startswith('UC'):
                    channel_id_to_use = await self.get_channel_id_by_username(channel_id_to_use)
                    if channel_id_to_use:
                        self.channel_id = channel_id_to_use
                    else:
                        return None
                
                url = f"{self.base_url}/channels"
                params = {
                    'part': 'contentDetails',
                    'id': self.channel_id,
                    'key': self.api_key
                }
                
                data = await self.make_request_with_retry('GET', url, params=params)
                if data and data.get('items'):
                    self._uploads_playlist_id = data['items'][0]['contentDetails']['relatedPlaylists']['uploads']
                    return self._uploads_playlist_id
                return None
            except YouTubeQuotaExceeded:
                raise
            except Exception as e:
                self.logger.error(f"Ошибка при получении uploads playlist ID: {e}")
                return None
        return self._uploads_playlist_id
    
    async def get_video_ids(self, limit: int = 5) -> List[str]:
        """Получает список ID видео с канала через uploads playlist (оптимизировано: 1 единица вместо 100)"""
        if not self.is_configured():
            self.logger.error("YouTube парсер не настроен")
            return []
        
        try:
            # Получаем ID плейлиста "Uploads" (кэшируется, username обрабатывается внутри)
            uploads_playlist_id = await self.get_uploads_playlist_id()
            if not uploads_playlist_id:
                self.logger.error("Не удалось получить uploads playlist ID")
                return []
            
            # Используем playlistItems.list вместо search.list (1 единица вместо 100!)
            url = f"{self.base_url}/playlistItems"
            params = {
                'part': 'contentDetails',
                'playlistId': uploads_playlist_id,
                'maxResults': limit,
                'key': self.api_key
            }
            
            data = await self.make_request_with_retry('GET', url, params=params)
            if data:
                video_ids = [item['contentDetails']['videoId'] for item in data.get('items', [])]
                return video_ids
            return []
        except YouTubeQuotaExceeded:
            # Пробрасываем исключение квоты дальше
            raise
        except Exception as e:
            self.logger.error(f"Ошибка при получении списка видео: {e}")
            return []
    
    async def get_video_comments(self, video_id: str, limit: int = 20) -> List[Comment]:
        """Получает комментарии к конкретному видео включая ответы"""
        try:
            url = f"{self.base_url}/commentThreads"
            params = {
                'part': 'snippet,replies',
                'videoId': video_id,
                'maxResults': limit,
                'order': 'time',
                'key': self.api_key
            }
            
            comments = []
            
            data = await self.make_request_with_retry('GET', url, params=params)
            if data:
                try:
                    for item in data.get('items', []):
                        top_comment = item['snippet']['topLevelComment']['snippet']
                        
                        published_at = datetime.fromisoformat(
                            top_comment['publishedAt'].replace('Z', '+00:00')
                        )
                        
                        # Используем timestamp комментария в секундах для параметра t
                        comment_timestamp = int(published_at.timestamp())
                        source_url = f"https://www.youtube.com/watch?v={video_id}&lc={item['id']}&t={comment_timestamp}s"
                        
                        comment = Comment(
                            author=top_comment['authorDisplayName'],
                            text=top_comment['textDisplay'],
                            source=self.source_name,
                            timestamp=published_at,
                            source_url=source_url
                        )
                        
                        comments.append(comment)
                        
                        if 'replies' in item and 'comments' in item['replies']:
                            for reply in item['replies']['comments']:
                                reply_snippet = reply['snippet']
                                
                                reply_published_at = datetime.fromisoformat(
                                    reply_snippet['publishedAt'].replace('Z', '+00:00')
                                )
                                
                                # Используем timestamp комментария в секундах для параметра t
                                reply_timestamp = int(reply_published_at.timestamp())
                                reply_source_url = f"https://www.youtube.com/watch?v={video_id}&lc={reply['id']}&t={reply_timestamp}s"
                                
                                reply_comment = Comment(
                                    author=reply_snippet['authorDisplayName'],
                                    text=f"↳ {reply_snippet['textDisplay']}",
                                    source=self.source_name,
                                    timestamp=reply_published_at,
                                    source_url=reply_source_url
                                )
                                
                                comments.append(reply_comment)
                except Exception as e:
                    self.logger.error(f"Ошибка обработки комментариев YouTube: {e}")
            
            return comments
        except YouTubeQuotaExceeded:
            # Пробрасываем исключение квоты дальше
            raise
        except Exception as e:
            self.logger.error(f"Ошибка при получении комментариев к видео {video_id}: {e}")
            return []
    
    async def get_comments(self, limit: int = 50) -> List[Comment]:
        """Получает комментарии из YouTube с оптимизацией"""
        if not self.is_configured():
            self.logger.error("YouTube парсер не настроен")
            return []
        
        all_comments = []
        
        try:
            import time
            start_time = time.time()
            
            # Получаем список видео
            video_ids = await self.get_video_ids(limit=20)
            video_ids_time = time.time() - start_time
            
            if not video_ids:
                return []
            
            # Параллельно получаем комментарии ко всем видео
            comments_start_time = time.time()
            tasks = [self.get_video_comments(video_id, limit=30) for video_id in video_ids]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            comments_time = time.time() - comments_start_time
            
            self.logger.debug(f"YouTube: получение списка видео: {video_ids_time:.2f}с, комментарии: {comments_time:.2f}с")
            
            # Обрабатываем результаты
            for i, result in enumerate(results):
                if isinstance(result, YouTubeQuotaExceeded):
                    # Если квота исчерпана, пробрасываем исключение дальше
                    raise result
                elif isinstance(result, Exception):
                    self.logger.error(f"Ошибка при получении комментариев к видео {video_ids[i]}: {result}")
                    continue
                all_comments.extend(result)
            
            # Сортируем по времени (новые сначала)
            all_comments.sort(key=lambda x: x.timestamp, reverse=True)
            
            return all_comments[:limit]
            
        except YouTubeQuotaExceeded:
            # Пробрасываем исключение квоты дальше, чтобы его можно было обработать в main.py
            raise
        except Exception as e:
            self.logger.error(f"Ошибка при получении комментариев: {e}")
            return []

# Функция для создания YouTube парсера
def create_youtube_parser(api_key: str, channel_id: str) -> YouTubeParser:
    """Создает и возвращает улучшенный YouTube парсер"""
    return YouTubeParser(api_key, channel_id)


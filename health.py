#!/usr/bin/env python3
"""
Health check endpoint для мониторинга состояния бота
"""

import logging
from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import threading
import time
from database import crm
from config import HEALTH_CHECK_PORT

logger = logging.getLogger(__name__)

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/health':
            self.handle_health_check()
        elif self.path == '/ready':
            self.handle_readiness_check()
        else:
            self.send_error(404)
    
    def handle_health_check(self):
        """Проверка здоровья приложения"""
        try:
            # Проверяем базовые компоненты
            health_status = {
                "status": "healthy",
                "timestamp": time.time(),
                "version": "1.0.0",
                "components": {
                    "database": self.check_database(),
                    "agents": self.check_agents()
                }
            }
            
            if all(health_status["components"].values()):
                self.send_response(200)
            else:
                self.send_response(503)
                
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(health_status, indent=2).encode())
            
        except Exception as e:
            logger.error(f"Ошибка при проверке здоровья: {e}")
            self.send_error(500)
    
    def handle_readiness_check(self):
        """Проверка готовности приложения к работе"""
        try:
            # Проверяем, что все критически важные компоненты готовы
            ready_status = {
                "status": "ready",
                "timestamp": time.time(),
                "checks": {
                    "database_loaded": len(crm.agents) > 0,
                    "agents_count": len(crm.agents)
                }
            }
            
            if ready_status["checks"]["database_loaded"]:
                self.send_response(200)
            else:
                self.send_response(503)
                
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(ready_status, indent=2).encode())
            
        except Exception as e:
            logger.error(f"Ошибка при проверке готовности: {e}")
            self.send_error(500)
    
    def check_database(self):
        """Проверка состояния базы данных"""
        try:
            return len(crm.agents) > 0
        except Exception:
            return False
    
    def check_agents(self):
        """Проверка загруженности агентов"""
        try:
            return len(crm.agents) > 0
        except Exception:
            return False
    
    def log_message(self, format, *args):
        """Отключаем стандартное логирование HTTP запросов"""
        pass

def start_health_server(host='0.0.0.0', port=None):
    """Запуск сервера health check в отдельном потоке"""
    if port is None:
        port = HEALTH_CHECK_PORT
        
    try:
        server = HTTPServer((host, port), HealthCheckHandler)
        
        def run_server():
            server.serve_forever()
        
        health_thread = threading.Thread(target=run_server, daemon=True)
        health_thread.start()
        
        return server
    except Exception as e:
        logger.error(f"Не удалось запустить health check сервер: {e}")
        return None

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Google Maps KML 下載工具
從 Google My Maps 分享連結自動下載 KML 檔案
"""

import requests
import re
import sys
import os
import xml.etree.ElementTree as ET
import csv
import html
from typing import Optional, List, Dict, Any, Tuple
from urllib.parse import urlparse, parse_qs


class KMLParser:
    """KML 解析器 - 提取 Placemark 資料並清理 HTML 標籤"""

    def __init__(self):
        self.placemarks = []
        self.ns = {'kml': 'http://www.opengis.net/kml/2.2'}

    def clean_html_tags(self, text: str) -> str:
        """移除 HTML 標籤並解碼實體"""
        if not text:
            return ""
        text = re.sub(r'<!\[CDATA\[(.*?)\]\]>', r'\1', text, flags=re.DOTALL)
        text = re.sub(r'<[^>]+>', '', text)
        text = html.unescape(text)
        return re.sub(r'\s+', ' ', text).strip()

    def parse_coordinates(self, coord_text: str) -> Tuple[Optional[float], Optional[float]]:
        """解析座標字串，回傳 (latitude, longitude)"""
        if not coord_text or not coord_text.strip():
            return None, None
        try:
            parts = coord_text.strip().split(',')
            if len(parts) >= 2:
                return float(parts[1]), float(parts[0])  # lat, lng
            return None, None
        except (ValueError, IndexError):
            print(f"⚠️  無法解析座標: {coord_text}")
            return None, None

    def extract_placemarks_from_kml(self, kml_file: str) -> List[Dict[str, Any]]:
        """從 KML 檔案提取所有 Placemark 資料"""
        self.placemarks = []
        try:
            tree = ET.parse(kml_file)
            root = tree.getroot()

            def process_element(element, folder_path=""):
                if element.tag.endswith('Folder'):
                    folder_name_elem = element.find('kml:name', self.ns)
                    folder_name = folder_name_elem.text if folder_name_elem is not None else ""
                    current_folder_path = f"{folder_path}/{folder_name}" if folder_path else folder_name
                    for child in element:
                        process_element(child, current_folder_path)
                elif element.tag.endswith('Placemark'):
                    data = {'folder': folder_path if folder_path else "根目錄"}

                    name_elem = element.find('kml:name', self.ns)
                    data['name'] = name_elem.text if name_elem is not None else ""

                    desc_elem = element.find('kml:description', self.ns)
                    raw_description = desc_elem.text if desc_elem is not None else ""
                    data['description'] = self.clean_html_tags(raw_description)

                    style_elem = element.find('kml:styleUrl', self.ns)
                    data['style_url'] = style_elem.text if style_elem is not None else ""

                    coord_elem = element.find('.//kml:coordinates', self.ns)
                    if coord_elem is not None:
                        latitude, longitude = self.parse_coordinates(coord_elem.text)
                        data['latitude'] = latitude
                        data['longitude'] = longitude
                    else:
                        data['latitude'] = None
                        data['longitude'] = None

                    self.placemarks.append(data)
                else:
                    for child in element:
                        process_element(child, folder_path)

            process_element(root)
        except (ET.ParseError, FileNotFoundError) as e:
            print(f"❌ 檔案錯誤: {e}")
            return []
        return self.placemarks

    def save_to_csv(self, output_file: str, placemarks: Optional[List[Dict[str, Any]]] = None):
        data_to_save = placemarks if placemarks is not None else self.placemarks
        if not data_to_save:
            print("❌ 沒有資料可以儲存")
            return
        try:
            with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=['folder', 'name', 'description', 'style_url', 'latitude', 'longitude'])
                writer.writeheader()
                for placemark in data_to_save:
                    writer.writerow(placemark)
            print(f"✅ 成功儲存 {len(data_to_save)} 筆 Placemark 資料到 {output_file}")
        except IOError as e:
            print(f"❌ 儲存檔案錯誤: {e}")

    def show_summary(self, placemarks: Optional[List[Dict[str, Any]]] = None):
        data_to_show = placemarks if placemarks is not None else self.placemarks
        if not data_to_show:
            print("❌ 沒有找到任何 Placemark 資料")
            return
        total_count = len(data_to_show)
        with_coords = sum(1 for p in data_to_show if p['latitude'] is not None and p['longitude'] is not None)
        without_coords = total_count - with_coords
        print(f"\n📊 處理結果摘要:")
        print(f"總共找到: {total_count} 個 Placemark")
        print(f"有座標: {with_coords} 個")
        print(f"無座標: {without_coords} 個")
        if without_coords > 0:
            print(f"\n⚠️  以下 {without_coords} 個 Placemark 沒有座標:")
            for i, placemark in enumerate(data_to_show, 1):
                if placemark['latitude'] is None or placemark['longitude'] is None:
                    print(f"  {i}. {placemark['name']}")

    def get_placemarks(self) -> List[Dict[str, Any]]:
        return self.placemarks


class GoogleMapsKMLDownloader:
    """Google Maps KML 下載器"""

    def __init__(self):
        self.session = requests.Session()
        # 設定 User-Agent 避免被阻擋
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })

    def extract_map_id(self, url: str) -> Optional[str]:
        """從 Google Maps URL 中提取 map ID"""
        try:
            # 解析 URL 參數
            parsed_url = urlparse(url)
            query_params = parse_qs(parsed_url.query)

            # 檢查是否有 mid 參數
            if 'mid' in query_params:
                map_id = query_params['mid'][0]
                print(f"✅ 成功提取 Map ID: {map_id}")
                return map_id

            # 如果沒有 mid 參數，嘗試從 URL 路徑中提取
            path_parts = parsed_url.path.split('/')
            for i, part in enumerate(path_parts):
                if part == 'd' and i + 1 < len(path_parts):
                    map_id = path_parts[i + 1]
                    print(f"✅ 從路徑提取 Map ID: {map_id}")
                    return map_id

            print("❌ 無法從 URL 中提取 Map ID")
            return None

        except Exception as e:
            print(f"❌ URL 解析錯誤: {e}")
            return None

    def build_kml_download_url(self, map_id: str) -> str:
        """構建 KML 下載 URL"""
        # Google My Maps KML 匯出的標準格式，加上 forcekml=1 強制 KML 格式
        base_url = "https://www.google.com/maps/d/kml"
        kml_url = f"{base_url}?mid={map_id}&forcekml=1"
        print(f"🔗 KML 下載連結: {kml_url}")
        return kml_url

    def download_kml(self, url: str, output_file: str = "data.kml") -> bool:
        """下載 KML 檔案"""
        try:
            print(f"🔄 開始下載 KML 檔案...")

            response = self.session.get(url, timeout=30)
            response.raise_for_status()

            # 檢查回應內容是否為 KML 格式
            content_type = response.headers.get('content-type', '').lower()
            if 'xml' not in content_type and 'kml' not in content_type:
                # 檢查內容是否包含 KML 標籤
                if not ('<kml' in response.text.lower() or '<?xml' in response.text.lower()):
                    print("⚠️  警告：下載的內容可能不是有效的 KML 檔案")
                    print(f"   Content-Type: {content_type}")
                    print(f"   內容預覽: {response.text[:200]}...")

            # 儲存檔案
            with open(output_file, 'w', encoding='utf-8') as file:
                file.write(response.text)

            file_size = os.path.getsize(output_file)
            print(f"✅ 成功下載 KML 檔案: {output_file}")
            print(f"📊 檔案大小: {file_size} bytes")

            # 顯示檔案內容預覽
            with open(output_file, 'r', encoding='utf-8') as file:
                preview = file.read(300)
                print(f"📄 檔案內容預覽:")
                print("-" * 50)
                print(preview)
                if len(preview) >= 300:
                    print("...")
                print("-" * 50)

            return True

        except requests.exceptions.RequestException as e:
            print(f"❌ 下載失敗: {e}")
            return False
        except IOError as e:
            print(f"❌ 檔案寫入失敗: {e}")
            return False
        except Exception as e:
            print(f"❌ 未預期的錯誤: {e}")
            return False

    def download_from_maps_url(self, maps_url: str, output_file: str = "data.kml") -> bool:
        """從 Google Maps URL 下載 KML"""
        print(f"🗺️  Google Maps KML 下載工具")
        print(f"📂 來源 URL: {maps_url}")
        print(f"💾 輸出檔案: {output_file}")
        print("=" * 60)

        # 提取 Map ID
        map_id = self.extract_map_id(maps_url)
        if not map_id:
            return False

        # 構建 KML 下載 URL
        kml_url = self.build_kml_download_url(map_id)

        # 下載 KML
        return self.download_kml(kml_url, output_file)

    def download_and_parse_to_csv(self, maps_url: str, kml_file: str = "data.kml", csv_file: str = "placemarks.csv") -> bool:
        """下載 KML 並解析為 CSV"""
        print(f"🗺️  Google Maps KML 下載並解析工具")
        print(f"📂 來源 URL: {maps_url}")
        print(f"💾 KML 檔案: {kml_file}")
        print(f"📊 CSV 檔案: {csv_file}")
        print("=" * 60)

        # 下載 KML
        if not self.download_from_maps_url(maps_url, kml_file):
            return False

        # 解析 KML 並轉為 CSV
        print(f"\n🔄 正在解析 KML 檔案...")
        kml_parser = KMLParser()
        placemarks = kml_parser.extract_placemarks_from_kml(kml_file)
        kml_parser.show_summary()

        if placemarks:
            print(f"\n💾 正在儲存為 CSV...")
            kml_parser.save_to_csv(csv_file)
            return True
        else:
            print("❌ 沒有資料可以儲存")
            return False


def main():
    print("🗺️  Google Maps KML 下載並解析工具")
    print("=" * 60)

    # 預設的 Google Maps URL
    default_url = "https://www.google.com/maps/d/u/0/viewer?ll=23.67227849999999%2C121.4284911&z=13&mid=1qOHK91tv68NacIN1GVTDYKn10ojb-t8"

    # 檢查命令列參數
    if len(sys.argv) > 1:
        mode = sys.argv[1]
        if mode in ['--csv', '-c']:
            # CSV 模式：下載並解析為 CSV
            maps_url = sys.argv[2] if len(sys.argv) > 2 else default_url
            kml_file = sys.argv[3] if len(sys.argv) > 3 else "data.kml"
            csv_file = sys.argv[4] if len(sys.argv) > 4 else "placemarks.csv"

            if maps_url == default_url:
                print(f"ℹ️  使用預設 URL")

            try:
                downloader = GoogleMapsKMLDownloader()
                success = downloader.download_and_parse_to_csv(maps_url, kml_file, csv_file)

                if success:
                    print("\n🎉 下載並解析完成！")
                    print(f"📁 KML 檔案: {os.path.abspath(kml_file)}")
                    print(f"📊 CSV 檔案: {os.path.abspath(csv_file)}")
                else:
                    print("\n❌ 處理失敗")
                    sys.exit(1)
            except KeyboardInterrupt:
                print("\n⚠️  用戶中斷操作")
            except Exception as e:
                print(f"\n❌ 發生未預期的錯誤: {e}")
                sys.exit(1)
        else:
            # URL 作為第一個參數，只下載 KML
            maps_url = mode
            output_file = sys.argv[2] if len(sys.argv) > 2 else "data.kml"

            try:
                downloader = GoogleMapsKMLDownloader()
                success = downloader.download_from_maps_url(maps_url, output_file)

                if success:
                    print("\n🎉 下載完成！")
                    print(f"📁 檔案位置: {os.path.abspath(output_file)}")
                else:
                    print("\n❌ 下載失敗")
                    sys.exit(1)
            except KeyboardInterrupt:
                print("\n⚠️  用戶中斷操作")
            except Exception as e:
                print(f"\n❌ 發生未預期的錯誤: {e}")
                sys.exit(1)
    else:
        # 沒有參數，使用預設 URL 並下載解析為 CSV
        maps_url = default_url
        print(f"ℹ️  使用預設 URL")
        print(f"ℹ️  將自動下載並解析為 CSV")

        try:
            downloader = GoogleMapsKMLDownloader()
            success = downloader.download_and_parse_to_csv(maps_url)

            if success:
                print("\n🎉 下載並解析完成！")
                print(f"📁 KML 檔案: {os.path.abspath('data.kml')}")
                print(f"📊 CSV 檔案: {os.path.abspath('placemarks.csv')}")
            else:
                print("\n❌ 處理失敗")
                sys.exit(1)
        except KeyboardInterrupt:
            print("\n⚠️  用戶中斷操作")
        except Exception as e:
            print(f"\n❌ 發生未預期的錯誤: {e}")
            sys.exit(1)


if __name__ == "__main__":
    main()
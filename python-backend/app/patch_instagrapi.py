"""
Monkey patch for instagrapi to fix multiple bugs:
1. extract_user_gql() TypeError - update_headers argument doesn't exist
2. KeyError: 'pinned_channels_info' - missing key in user data
3. Pydantic ValidationError: bio_links.0.link_id - missing required field
"""
_patch_applied = False


def patch_instagrapi():
    global _patch_applied
    if _patch_applied:
        return True
    try:
        from instagrapi.mixins.user import UserMixin
        import json
        import instagrapi.extractors

        if hasattr(UserMixin.user_info_by_username_gql, "_instagrapi_patched"):
            _patch_applied = True
            return True

        original_extract_broadcast_channel = instagrapi.extractors.extract_broadcast_channel
        original_extract_user_gql = instagrapi.extractors.extract_user_gql

        def patched_extract_broadcast_channel(data):
            try:
                if "pinned_channels_info" not in data:
                    return []
                if "pinned_channels_list" not in data.get("pinned_channels_info", {}):
                    return []
                return original_extract_broadcast_channel(data)
            except KeyError:
                return []

        def patched_extract_user_gql(data, **kwargs):
            try:
                if "pinned_channels_info" in data:
                    data = dict(data)
                    data["broadcast_channel"] = patched_extract_broadcast_channel(data)
                else:
                    data = dict(data)
                    data["broadcast_channel"] = []
            except Exception:
                data = dict(data) if isinstance(data, dict) else data
                if isinstance(data, dict):
                    data["broadcast_channel"] = []
            if isinstance(data, dict) and "bio_links" in data and isinstance(data["bio_links"], list):
                data["bio_links"] = [
                    link for link in data["bio_links"]
                    if isinstance(link, dict) and "link_id" in link
                ]
            return original_extract_user_gql(data)

        def patched_user_info_by_username_gql(self, username: str):
            username = str(username).lower()
            temporary_public_headers = {
                "Host": "www.instagram.com",
                "X-Requested-With": "XMLHttpRequest",
                "Sec-Ch-Prefers-Color-Scheme": "dark",
                "Sec-Ch-Ua-Platform": '"Linux"',
                "X-Ig-App-Id": "936619743392459",
                "Sec-Ch-Ua-Model": '""',
                "Sec-Ch-Ua-Mobile": "?0",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.112 Safari/537.36",
                "Accept": "*/*",
                "X-Asbd-Id": "129477",
                "Sec-Fetch-Site": "same-origin",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Dest": "empty",
                "Referer": "https://www.instagram.com/",
                "Accept-Language": "en-US,en;q=0.9",
                "Priority": "u=1, i",
            }
            instagrapi.extractors.extract_broadcast_channel = patched_extract_broadcast_channel
            instagrapi.extractors.extract_user_gql = patched_extract_user_gql
            try:
                response = self.public_request(
                    f"https://www.instagram.com/api/v1/users/web_profile_info/?username={username}",
                    headers=temporary_public_headers,
                )
                data = json.loads(response)["data"]["user"]
                if "pinned_channels_info" in data:
                    data["broadcast_channel"] = patched_extract_broadcast_channel(data)
                else:
                    data["broadcast_channel"] = []
                if "bio_links" in data and isinstance(data["bio_links"], list):
                    data["bio_links"] = [
                        link for link in data["bio_links"]
                        if isinstance(link, dict) and "link_id" in link
                    ]
                return patched_extract_user_gql(data)
            finally:
                instagrapi.extractors.extract_broadcast_channel = original_extract_broadcast_channel
                instagrapi.extractors.extract_user_gql = original_extract_user_gql

        patched_user_info_by_username_gql._instagrapi_patched = True
        UserMixin.user_info_by_username_gql = patched_user_info_by_username_gql
        _patch_applied = True
        return True
    except Exception as e:
        try:
            print(f"Warning: instagrapi patch failed: {e}")
        except Exception:
            pass
        return False

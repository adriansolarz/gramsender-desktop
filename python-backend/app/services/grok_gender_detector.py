import os
import requests
import base64
from typing import Optional, Dict, Any
import json

class GrokGenderDetector:
    """Service to detect gender using Grok AI based on profile picture, name, and bio"""
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("GROK_API_KEY")
        self.api_base = os.getenv("GROK_API_BASE", "https://api.x.ai/v1")
        self.enabled = bool(self.api_key)
    
    def download_image(self, image_url: str) -> Optional[bytes]:
        """Download image from URL and return as bytes"""
        try:
            response = requests.get(image_url, timeout=10, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            })
            response.raise_for_status()
            return response.content
        except Exception as e:
            print(f"Error downloading image: {e}")
            return None
    
    def analyze_profile_picture(self, image_url: str) -> Dict[str, Any]:
        """Use Grok vision API to analyze profile picture"""
        if not self.enabled:
            return {"gender": "unknown", "confidence": 0.0, "reason": "Grok API not configured"}
        
        try:
            # Download image
            image_data = self.download_image(image_url)
            if not image_data:
                return {"gender": "unknown", "confidence": 0.0, "reason": "Could not download image"}
            
            # Encode image to base64
            image_base64 = base64.b64encode(image_data).decode('utf-8')
            
            # Prepare Grok API request
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            }
            
            # Grok API structure (adjust based on actual API documentation)
            payload = {
                "model": "grok-2-vision-1212",  # Updated to vision model
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": """Analyze this Instagram profile picture and determine the person's likely gender based on visual presentation, clothing, hairstyle, and other visual indicators. 

Respond ONLY with valid JSON in this exact format:
{
  "gender": "male" or "female" or "unknown",
  "confidence": 0.0 to 1.0,
  "indicators": ["list", "of", "visual", "indicators"]
}"""
                            },
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": f"data:image/jpeg;base64,{image_base64}"
                                }
                            }
                        ]
                    }
                ],
                "max_tokens": 200,
                "temperature": 0.3
            }
            
            response = requests.post(
                f"{self.api_base}/chat/completions",
                headers=headers,
                json=payload,
                timeout=30
            )
            response.raise_for_status()
            
            result = response.json()
            content = result["choices"][0]["message"]["content"]
            
            # Parse JSON response
            try:
                # Extract JSON from response (might have markdown code blocks)
                content = content.strip()
                if content.startswith("```"):
                    content = content.split("```")[1]
                    if content.startswith("json"):
                        content = content[4:]
                content = content.strip()
                
                analysis = json.loads(content)
                return {
                    "gender": analysis.get("gender", "unknown").lower(),
                    "confidence": float(analysis.get("confidence", 0.0)),
                    "indicators": analysis.get("indicators", []),
                    "source": "profile_picture"
                }
            except (json.JSONDecodeError, ValueError) as e:
                # Fallback: try to extract gender from text
                content_lower = content.lower()
                if "male" in content_lower or "man" in content_lower:
                    return {"gender": "male", "confidence": 0.6, "source": "profile_picture", "method": "text_fallback"}
                elif "female" in content_lower or "woman" in content_lower:
                    return {"gender": "female", "confidence": 0.6, "source": "profile_picture", "method": "text_fallback"}
                else:
                    return {"gender": "unknown", "confidence": 0.0, "source": "profile_picture", "error": str(e)}
                    
        except requests.exceptions.RequestException as e:
            print(f"Error calling Grok API for profile picture: {e}")
            return {"gender": "unknown", "confidence": 0.0, "reason": f"API error: {str(e)}"}
        except Exception as e:
            print(f"Error analyzing profile picture with Grok: {e}")
            return {"gender": "unknown", "confidence": 0.0, "reason": str(e)}
    
    def analyze_bio(self, bio_text: str) -> Dict[str, Any]:
        """Analyze bio text for gender indicators including pronouns"""
        if not self.enabled or not bio_text:
            return {"gender": "unknown", "confidence": 0.0}
        
        try:
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            }
            
            # Pronoun detection (quick check first)
            pronouns = {
                "male": ["he/him", " he ", " him ", " his ", "himself"],
                "female": ["she/her", " she ", " her ", " hers ", "herself"],
            }
            
            bio_lower = bio_text.lower()
            found_pronouns = []
            for gender, pronoun_list in pronouns.items():
                for pronoun in pronoun_list:
                    if pronoun in bio_lower:
                        found_pronouns.append((gender, pronoun))
                        break  # Found one, move to next gender
            
            # If clear pronouns found, use them (high confidence)
            if found_pronouns:
                pronoun_gender = found_pronouns[0][0]
                return {
                    "gender": pronoun_gender,
                    "confidence": 0.9,
                    "pronouns_found": [p[1].strip() for p in found_pronouns],
                    "source": "bio_pronouns"
                }
            
            # Use Grok to analyze bio for gendered language
            payload = {
                "model": "grok-beta",  # Adjust model name
                "messages": [
                    {
                        "role": "user",
                        "content": f"""Analyze this Instagram bio and determine if the language suggests the person is male, female, or if it's unclear. 

Look for:
- Pronouns (he/him, she/her, they/them)
- Gendered language patterns
- Feminine/masculine indicators in the writing style
- Any explicit gender mentions

Bio: "{bio_text}"

Respond ONLY with valid JSON in this exact format:
{{
  "gender": "male" or "female" or "unknown",
  "confidence": 0.0 to 1.0,
  "pronouns_found": ["list", "of", "pronouns"],
  "indicators": ["list", "of", "language", "indicators"]
}}"""
                    }
                ],
                "max_tokens": 200,
                "temperature": 0.3
            }
            
            response = requests.post(
                f"{self.api_base}/chat/completions",
                headers=headers,
                json=payload,
                timeout=15
            )
            response.raise_for_status()
            
            result = response.json()
            content = result["choices"][0]["message"]["content"]
            
            # Parse JSON response
            try:
                # Extract JSON from response
                content = content.strip()
                if content.startswith("```"):
                    content = content.split("```")[1]
                    if content.startswith("json"):
                        content = content[4:]
                content = content.strip()
                
                analysis = json.loads(content)
                
                analysis["gender"] = analysis.get("gender", "unknown").lower()
                analysis["confidence"] = float(analysis.get("confidence", 0.0))
                analysis["source"] = "bio_ai"
                return analysis
            except (json.JSONDecodeError, ValueError) as e:
                # Fallback: use simple pronoun detection
                return {"gender": "unknown", "confidence": 0.0, "source": "bio", "error": str(e)}
                
        except requests.exceptions.RequestException as e:
            print(f"Error calling Grok API for bio: {e}")
            # Fallback to simple pronoun detection
            bio_lower = bio_text.lower()
            if any(p in bio_lower for p in ["he/him", " he ", " him ", " his "]):
                return {"gender": "male", "confidence": 0.7, "source": "bio", "method": "pronoun_fallback"}
            elif any(p in bio_lower for p in ["she/her", " she ", " her ", " hers "]):
                return {"gender": "female", "confidence": 0.7, "source": "bio", "method": "pronoun_fallback"}
            return {"gender": "unknown", "confidence": 0.0, "source": "bio"}
        except Exception as e:
            print(f"Error analyzing bio with Grok: {e}")
            return {"gender": "unknown", "confidence": 0.0, "source": "bio"}
    
    def detect_gender(self, profile_pic_url: Optional[str] = None, 
                     full_name: str = "", first_name: str = "",
                     bio_text: str = "") -> Dict[str, Any]:
        """
        Comprehensive gender detection using all available data sources.
        Returns the most confident result.
        """
        results = []
        
        # 1. Analyze bio first (fastest, often most reliable with pronouns)
        if bio_text:
            bio_result = self.analyze_bio(bio_text)
            if bio_result.get("gender") != "unknown":
                results.append(bio_result)
        
        # 2. Analyze profile picture (high confidence if available)
        if profile_pic_url:
            pic_result = self.analyze_profile_picture(profile_pic_url)
            if pic_result.get("gender") != "unknown":
                results.append(pic_result)
        
        # 3. Fallback to name-based detection (from existing function)
        from ..instagram_worker import detect_gender_from_name
        name_result = detect_gender_from_name(full_name, first_name)
        if name_result != "unknown":
            results.append({
                "gender": name_result,
                "confidence": 0.5,
                "source": "name"
            })
        
        # Return most confident result, or combine results
        if not results:
            return {"gender": "unknown", "confidence": 0.0, "sources": []}
        
        # Sort by confidence
        results.sort(key=lambda x: x.get("confidence", 0.0), reverse=True)
        best_result = results[0].copy()
        
        # If multiple sources agree, boost confidence
        if len(results) > 1:
            genders = [r["gender"] for r in results if r["gender"] != "unknown"]
            if len(set(genders)) == 1:  # All agree
                best_result["confidence"] = min(1.0, best_result.get("confidence", 0.5) + 0.2)
                best_result["sources"] = [r["source"] for r in results]
                best_result["all_sources_agree"] = True
            else:
                best_result["sources"] = [r["source"] for r in results]
                best_result["conflicting_sources"] = True
        
        return best_result

    def extract_first_name(self, full_name: str = "", username: str = "") -> str:
        """Use Grok to infer first name from full name and/or username. Returns empty string if disabled or error."""
        if not self.enabled:
            return ""
        if not (full_name or username):
            return ""
        try:
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            }
            prompt = f"""Given this person's full name and/or Instagram username, reply with ONLY their first name (one word, capitalized). No explanation.
Full name: {full_name or 'unknown'}
Username: {username or 'unknown'}"""
            payload = {
                "model": "grok-2-1212",
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 30,
                "temperature": 0.1
            }
            response = requests.post(
                f"{self.api_base}/chat/completions",
                headers=headers,
                json=payload,
                timeout=15
            )
            response.raise_for_status()
            result = response.json()
            content = (result.get("choices") or [{}])[0].get("message", {}).get("content", "").strip()
            if content:
                return content.split()[0].strip().title() if content.split() else ""
            return ""
        except Exception as e:
            print(f"Grok extract_first_name error: {e}")
            return ""

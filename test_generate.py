"""
Test script: parse cookie + fetch access token + generate 1 ảnh.
Chạy trên VPS: cd backend && python ..\test_generate.py
"""
import sys, os, json
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "backend"))

os.environ["AUTO_RECAPTCHA"] = "1"
os.environ["RECAPTCHA_MODE"] = "selenium"
os.environ["SELENIUM_HEADLESS"] = "0"

CHROME_PROFILE = r"C:\BananaPro\chrome_profile"

COOKIE_JSON = """[
    {"name": "__Host-next-auth.csrf-token", "value": "dd8c7c41646d06e19bc756ad7361ea388b04550d9006e30c93525809879cb635%7Cc9a10ddb79f6824abb2cb77ed847080937fa8022a1adc6f977fa70d6422b0f30"},
    {"name": "__Secure-next-auth.callback-url", "value": "https%3A%2F%2Flabs.google%2Ffx%2Ftools%2Fflow"},
    {"name": "__Secure-next-auth.session-token", "value": "eyJhbGciOiJkaXIiLCJlbmMiOiJBMjU2R0NNIn0.._dfFn4YDTkIIbx1k.XDEofLRmYGyv_ux5VVGwSSVzwMuDvD90KKB11qfB51FzHI8a1jnGk_mgitofIJ-0c7-c6SeoLgSeIW-9Fq9NztKGzCQD2PI0UsVokcoXQQ1Kg5IWgRFh33B43Pg48umtu7bjqzGXZnPJiRvwO3SAn7aS7IkOMBr5fcGQp82Agh5qZeqJE-Q-JCv7GC7B7FTSOgmVkTOC3c8f-2dS0cZqYvBx2ARGFB5yzX-4qpn-qOaeUgwfUTCc7qlBOeK3kewcnzhasjR8T6qug7fflvWU1AwdtM9Y-usi_Bw-FwUc6xjHaQUgl52Ebggpzrn4K9ACh2AsXbUkwE300n8ovwXvnqmL8xorIRmG00olopzaMK59tvDaFWpYqJV_Ug1225NxPgMY5NAVkJhFN-74jWYi50mJR4G5oLEZK56Y9QAc_xhZUdUPrP93ZSs07ezoE-03TlBq3sBC3aiOz6Vk2ctynuSZhNtPtaCMqQv5t5uCaYQ_RIF3Zc58cS_QRJYJyAop0vmUqn00_0phxCTo2t3W9iTDsYRzs3Xzamnla08LfB_KCIl3f8TUT1QYXnP4m2gECKt9cgxrxMgmOgDdT9cOly1WyKW09QGOsF1QsVtPK-AHQE0uCS8xqE2RC5im1ike7f8fDUMANhuni9-Q-PVc3UikPq-GmeDfx_G9GFLFnDLSJJ_-7MGCYRX5yCnPKnxhtW0NtfU8rxCgsExyTVDGvdloR9q5nfc4jkcT3b1GEq-xPXAdH0jSxYec7867dcvYK_xmXjTqvvTxiCCljxDg1th3TjGJ00W0ZDGI8Wa_3IbfNuTGapKYEyToBQyLp-M7FdG67g08ZVBI7xZla4hChXDXyN-XouQS1QcfCqwI-nIYVCsdMn2zsbQgjgzc97o4WmWoPgpkuChsk_K2QVzCKAZCp2hGnQ-31LtXuJfEARDgdNOi73_CkrExxRi1WXuLgFjK18GCySwsAvweN1gVkcLIty6hZoYMcKjsR6q7.FEhtCsA10RKVmzNKpSaJag"}
]"""

from complete_flow import LabsFlowClient

cookies = {c["name"]: c["value"] for c in json.loads(COOKIE_JSON)}
print(f"✅ Parsed {len(cookies)} cookies")

client = LabsFlowClient(cookies, profile_path=CHROME_PROFILE)

print("\n--- Fetch access token ---")
ok = client.fetch_access_token()
if not ok:
    print("❌ Cookie hết hạn")
    sys.exit(1)
print(f"✅ Token OK: {client.access_token[:30]}...")

print("\n--- Create workflow ---")
workflow_id = client.create_whisk_workflow()
print(f"Workflow ID: {workflow_id}")
if not workflow_id:
    print("❌ Không tạo được workflow:", client.last_error_detail)
    sys.exit(1)

print("\n--- Generate image ---")
result = client.generate_image_from_text(
    workflow_id=workflow_id,
    prompt="a cute cat",
    image_model="NARWHAL",
    aspect_ratio="16:9",
)
print(f"\nResult: {json.dumps(result, indent=2) if result else 'None - ' + str(client.last_error_detail)}")

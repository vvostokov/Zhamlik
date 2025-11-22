from playwright.sync_api import sync_playwright

def test_app():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)  # Headless for CI
        page = browser.new_page()
        
        try:
            # Test Login
            page.goto("http://127.0.0.1:5001/login")
            page.fill("#username", "admin")
            page.fill("#password", "admin")
            page.click("button[type='submit']")
            page.wait_for_url("http://127.0.0.1:5001/")  # Should redirect to index
            print("Login test passed")
            
            # Test Debts Page
            page.goto("http://127.0.0.1:5001/debts")
            assert "Долги и Регулярные Платежи" in page.content()
            print("Debts page loaded successfully")
            
            # Test Analytics Page
            page.goto("http://127.0.0.1:5001/analytics")
            assert "Аналитика" in page.title() or "Analytics" in page.content()  # Assuming title or content
            print("Analytics page loaded successfully")
            
            print("All tests passed!")
            
        except Exception as e:
            print(f"Test failed: {e}")
            import traceback
            traceback.print_exc()
        finally:
            browser.close()

if __name__ == "__main__":
    test_app()
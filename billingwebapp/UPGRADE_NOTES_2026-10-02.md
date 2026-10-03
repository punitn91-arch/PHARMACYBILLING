# Upgrade Notes – 2 Oct 2026

Review me jo problems mili thi, unke fixes aur naye features. Saare 228 automated tests pass hain (212 purane + 16 naye).

## Deploy karne se PEHLE (zaroori)

1. **`SECRET_KEY` set karo** (Railway → Variables). Kam se kam 32 random characters:
   `python -c "import secrets; print(secrets.token_urlsafe(48))"`
   Production me key na ho, ya placeholder ho, to app **start nahi hogi** (ye jaanbujhkar kiya hai).
   Agar pehle key set nahi thi (app `dev-secret` use kar raha tha), to naya key lagane par sab users ek baar logout honge,
   aur pehle WhatsApp par bheje gaye invoice links kaam karna band kar denge.
2. **Database migrate karo:** `flask db upgrade`. App start hote waqt bhi naye columns khud add ho jaate hain, isliye bhool jao to bhi app chalega.
   Pehle migrations me ek bug tha: do "heads" the, jisse `flask db upgrade` fail hota tha. Wo theek kar diya hai.
3. **Agar zip kisi ko bheja tha:** Twilio, Sarvam aur 2Factor ki API keys rotate karo.
   Aage se share karne ke liye `python billingwebapp/scripts/package_clean_zip.py` use karo.
   Ye `.env`, database, backups aur venv ko zip me nahi daalta.

## Kya badla

### Security
- `SECRET_KEY` ka `"dev-secret"` fallback hata diya. Local system par ek random key `instance/.secret_key` me save hoti hai.
- Har staff form aur AJAX request par CSRF protection (`static/csrf.js` token apne aap jodta hai).
- Delete/Archive actions (medicine, vendor, purchase, user, pending bill) ab sirf POST se hote hain. Link khulne se kuch delete nahi hota.
- Debug mode default OFF hai.

### Invoice: Delete ki jagah Cancel
- Invoice ab database se mitta nahi. **CANCELLED** mark hota hai, reason save hota hai, aur stock + vendor lot wapas aa jaata hai.
- Cancelled invoice sales, reports, dashboard aur GST me nahi gina jaata. Uska return nahi ho sakta aur use edit nahi kar sakte.
- Invoice list me "Cancel Invoice" button reason poochta hai.

### GST
- **Sahi formula:** MRP me GST included hai, isliye GST = amount × rate / (100 + rate). Pehle 5% upar se lagaya jaata tha, jo galat tha.
- **Har medicine ka apna GST%** (0/5/12/18/28/40) aur **Drug Schedule**: Medicine Master → Add/Edit me.
  Purani medicines me GST% unki purchase bill se ek baar apne aap bhar diya jaata hai.
  Purchase entry par bhi medicine ka GST% update hota hai.
- Har bill line par GST%, taxable value aur GST amount save hota hai. Invoice print par bhi dikhta hai.
- **GST Summary report** (`Reports → GST Summary`): rate-wise sales, returns (credit notes) aur purchases (input GST), Excel download ke saath.
  Ye sirf working summary hai; filing se pehle CA se verify karwa lena.

### Returns
- Har returned item ki **Condition** chuni jaati hai: *Good → wapas stock*, *Damaged* ya *Expired*.
  Damaged/Expired maal **Quarantine Stock** me jaata hai, bikne wale stock me nahi. Admin wahan se "Back to stock" ya "Write off" karta hai.
- `/return-medicine` page par bhi ab **15 din ki return window** aur **cold-chain block** lagu hai (admin override kar sakta hai).
- Manual return (bina invoice) ab vendor lot ki qty bhi theek karta hai, aur cancel karne par wapas ghatata hai.
- Medicine ki matching name + batch + **expiry** se hoti hai.

### Stock
- Billing me stock row lock hota hai, isliye do counters se ek saath aakhri strip bikne par stock minus nahi hoga.
- Same name + batch wali medicine dobara add nahi ho sakti.
- Purchase me cost (GST ke saath) MRP se zyada ho to warning dikhti hai.
- **Data Health** page (admin): duplicate rows ko merge karna, expired stock, stock vs lot mismatch aur cost > MRP.

### Compliance aur naye pages
- **Schedule H1/X:** aisi medicine ke bill me doctor ka naam zaroori hai. **Schedule H1 Register** report Excel ke saath.
- **Expiry Management:** expired, 30, 60 aur 90 din ke buckets, value, last vendor, aur expired stock ka write-off (vendor lots ke saath sync).
- Sidebar me naye links: Quarantine Stock, Expiry Management, Reorder List, GST Summary, Schedule H1 Register, Data Health.
- Automatic backup me naye tables (quarantine, return lots) bhi shamil hain.

## Jo jaanbujhkar abhi NAHI kiya

- **`app.py` ko modules me todna** (ab ~17,000 lines): ek hi baar me karna risky hai. Isse dheere-dheere, ek-ek module karke karna behtar hai.
  Naya GST code ek jagah (`split_inclusive_gst`) rakha gaya hai, taaki aage todna aasaan ho.
- **Cloud par off-site backup:** daily backup pehle se server disk par banta hai. Railway ka disk restart par mit sakta hai,
  isliye backup ko Google Drive/S3 par copy karna chahiye. Iske liye aapke cloud account ki access chahiye.
- **Stock Sale (B2B, purchase rate par)** me GST bhi rate ke andar maana gaya hai, retail bill ki tarah. Agar B2B me GST
  upar se lagna chahiye, to CA se confirm karke batana.

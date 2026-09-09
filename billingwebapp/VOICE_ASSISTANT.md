# Local voice receptionist

This is a local Mac test client for the existing Flask clinic application. It
reads only public appointment capacity and fees from the same database as the
web app. It does not bypass OTP protection for appointments or lab reports.
When `SARVAM_API_KEY` is present in the project's `.env`, it speaks replies
with Sarvam Bulbul v3; otherwise it uses the built-in Mac voice.

## One-time setup

```bash
cd /Users/punit/Desktop/billingwebapp/billingwebapp
source venv/bin/activate
python -m pip install -r requirements-voice.txt
```

Use the inner `billingwebapp/venv` shown above, not the outer `.venv` folder.

## Natural Hindi/Hinglish voice (Sarvam)

Add the key only to the local `.env` file beside `app.py`:

```env
SARVAM_API_KEY=your_private_key_here
```

The script reads this value directly and never prints it. Do not paste the key
into chat or commit `.env` to Git. The default natural Hindi voice is `priya`.

To use a different Sarvam voice or speaking speed:

```bash
python voice_receptionist.py --sarvam-speaker suhani --sarvam-pace 0.95
```

Use `--voice-provider sarvam` to try Sarvam explicitly, or `--voice-provider macos`
to temporarily use the built-in Mac voice. In the default `auto` mode, the
script uses Sarvam when the key is configured and safely falls back to Mac
speech if the network is unavailable.

## Reliable public Hindi/Hinglish recognition (Sarvam)

The default recognizer stays local. When the caller has consented to sending
**public, non-patient** microphone audio to Sarvam, run:

```bash
python voice_receptionist.py --enable-booking --language auto --stt-provider sarvam-public
```

Sarvam Saaras transcribes public Hindi, English, and Hinglish questions using
its code-mix mode. The WAV is held in memory for the request and is not written
to a project file. As soon as an appointment booking starts, cloud STT stops:
the date, gender, and confirmation are recognized locally on this Mac, while
the patient name, mobile number, and OTP are collected through hidden local
terminal input. Do not use this mode without the caller's appropriate consent.

## Secure voice appointment booking

Voice booking is deliberately off by default. Enable it only after a real SMS
OTP provider is configured, then run:

```bash
python voice_receptionist.py --enable-booking
```

The assistant supports **normal** first-come, first-served appointments only.
It asks for date, name, mobile, and gender, reviews the date/fee/arrival window
without repeating the patient's private details, and sends the existing SMS
OTP only after the caller says yes.

For a Hindi/Hinglish conversation, dates such as `कल`, `tomorrow`, `23 August`,
and `23-08-2026` are supported. Short booking answers such as `female`,
`महिला`, `male`, `पुरुष`, `other`, `हाँ`, and `yes` have step-specific speech
recognition hints.

If the microphone still misunderstands a booking answer, type only `t` at the
normal `>` prompt and press Enter. The next prompt is hidden, so a name or
mobile number is not echoed or stored in terminal history. At the hidden
gender prompt, `1=female`, `2=male`, and `3=other`; at the review prompt,
`y` sends the verification SMS and `c` cancels. Never type personal details or
an OTP directly at the normal `>` prompt.

When the SMS arrives, type its six digits into the hidden terminal prompt.
Never say an OTP aloud: spoken OTPs are not accepted, displayed, logged, or
sent to Sarvam. Type `resend` or `cancel` in that hidden prompt when needed.

The code fails closed if the OTP mode is missing, `development`, `console`, or
`test`; those modes do not deliver a real patient SMS. Use one of the existing
SMS transports in `.env` instead. It checks this before asking for patient
details. For example, 2Factor:

```env
PUBLIC_PORTAL_OTP_MODE=twofactor_sms
TWOFACTOR_API_KEY=your_private_2factor_key
PUBLIC_PORTAL_SHOW_DEV_OTP=0
```

Or an approved MSG91 Flow template:

```env
PUBLIC_PORTAL_OTP_MODE=msg91
MSG91_AUTH_KEY=your_private_msg91_key
MSG91_TEMPLATE_ID=your_approved_template_id
MSG91_OTP_VARIABLE=otp
PUBLIC_PORTAL_SHOW_DEV_OTP=0
```

Do not add provider keys to source code or chat. A priority request remains on
the existing secure web/reception path because staff payment verification is
required before it can become an appointment.

## Test the Flask connection without a microphone

```bash
python voice_receptionist.py --text "आज appointment मिलेगा?"
python voice_receptionist.py --text "मेरी thyroid report आ गई?"
```

To print a reply without generating audio:

```bash
python voice_receptionist.py --text "नमस्ते" --no-speak
```

## Start the voice receptionist

```bash
python voice_receptionist.py
```

The first run downloads Whisper's `base` model once. Press Enter, speak for up
to four seconds, and wait for the answer. If the fast `base` reading is
clear garbage or produces the generic reply, the same WAV is automatically
rechecked locally with Whisper's `small` model. That stronger model downloads
once on its first recovery and then remains cached. Type `q` to stop. Use
`--duration 10` only if callers need longer answers, or `--recovery-model none`
to disable the second local pass.

## Faster replies

For normal short questions, use fast capture mode:

```bash
python voice_receptionist.py --enable-booking --language auto --stt-provider sarvam-public --voice-provider sarvam --fast
```

Instead of always recording the complete four-second window, supported
microphones begin recognition about `0.85` seconds after the caller finishes
speaking. A one-second question therefore usually avoids roughly two seconds
of waiting. If the Mac microphone driver does not support low-latency
streaming, the launcher automatically keeps the reliable four-second path.
You can make the pause slightly longer for very slow speakers with
`--silence-seconds 1.2`; do not reduce it below `0.25`.

Sarvam voice requests also stop waiting after 10 seconds if the network is
unavailable, then safely use the built-in Mac voice. This affects outages only;
normal Sarvam replies keep their natural voice.

With the default local STT mode, Sarvam processes only the assistant's safe
reply text for speech generation. With the explicit `--stt-provider
sarvam-public` mode, it also receives the caller's public question audio. It
never receives booking audio, names, mobile numbers, OTPs, patient reports, or
appointment-creation details.

## Microphone troubleshooting

```bash
python voice_receptionist.py --list-devices
```

If no microphone appears, enable Visual Studio Code (or Terminal) in **System
Settings → Privacy & Security → Microphone**, then fully restart that app.

## Safety boundaries in this version

- Appointment availability and fees are live Flask data.
- The application uses first-come, first-served arrivals, not promised
  individual time slots.
- Voice appointment creation remains behind the application's existing SMS OTP,
  capacity-locking, and confirmation flows.
- The receptionist does not provide medicine or treatment advice.

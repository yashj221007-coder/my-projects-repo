import asyncio
import csv
import io
import logging
import random
import string
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ConversationHandler,
    ContextTypes,
    filters,
)
from sqlitedict import SqliteDict

# Enable logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# Configuration constants
TEACHER_PASSCODE = "12345"  # Change to your secure passcode
DB_FILE = "attendance_system.sqlite"

# Conversation states for Flow 0 Registration
FULL_NAME, SELECT_ROLE, TEACHER_PASS, STUDENT_ROLL = range(4)

# Active attendance session tracking (In-Memory RAM State)
ACTIVE_SESSION = {
    "active": False,
    "otp": None,
    "subject": None,
    "class_info": None,
    "teacher_id": None,
    "present_students": set(),
}

# ==================== ASYNC DATABASE & QUEUE LAYER ====================
db_write_queue = asyncio.Queue()


async def db_writer_worker():
    """Background worker that serializes all database writes sequentially to prevent SQLite locks."""
    while True:
        item = await db_write_queue.get()
        
        if len(item) == 2:
            user_id, data = item
            def _write_user():
                with SqliteDict(DB_FILE, autocommit=True) as db:
                    db[str(user_id)] = data
            await asyncio.to_thread(_write_user)
        elif len(item) == 1:
            session_data = item[0]
            def _write_session():
                with SqliteDict(DB_FILE, autocommit=True) as db:
                    sessions = db.get("history_sessions", [])
                    sessions.append(session_data)
                    db["history_sessions"] = sessions
            await asyncio.to_thread(_write_session)
            
        db_write_queue.task_done()


async def save_user_record(user_id: int, data: dict):
    """Instantly drops a write operation into the async queue."""
    await db_write_queue.put((user_id, data))


async def get_user_record(user_id: int):
    """Asynchronous disk read that avoids blocking the event loop."""
    def _read():
        with SqliteDict(DB_FILE) as db:
            return db.get(str(user_id))
    return await asyncio.to_thread(_read)


async def get_all_students():
    """Asynchronous batch read normalizing user_ids to integers for accurate reporting."""
    def _read_all():
        students = []
        with SqliteDict(DB_FILE) as db:
            for uid, data in db.items():
                if isinstance(data, dict) and data.get("flag") == "student":
                    students.append({
                        "user_id": int(uid),
                        "name": data.get("name"),
                        "roll_no": data.get("roll_no")
                    })
        students.sort(key=lambda s: str(s["roll_no"]))
        return students
    return await asyncio.to_thread(_read_all)


async def get_saved_subjects():
    """Fetches previously used subjects from SQLite."""
    def _read_subjects():
        with SqliteDict(DB_FILE) as db:
            return db.get("saved_subjects", [])
    return await asyncio.to_thread(_read_subjects)


async def add_saved_subject(subject: str):
    """Adds a new subject to the persistent SQLite list if it doesn't already exist."""
    def _write_subject():
        with SqliteDict(DB_FILE, autocommit=True) as db:
            subjects = db.get("saved_subjects", [])
            if subject not in subjects:
                subjects.append(subject)
                db["saved_subjects"] = subjects
    return await asyncio.to_thread(_write_subject)


async def get_saved_classes():
    """Fetches previously used classes/years from SQLite."""
    def _read_classes():
        with SqliteDict(DB_FILE) as db:
            return db.get("saved_classes", [])
    return await asyncio.to_thread(_read_classes)


async def add_saved_class(class_name: str):
    """Adds a new class to the persistent SQLite list if it doesn't already exist."""
    def _write_class():
        with SqliteDict(DB_FILE, autocommit=True) as db:
            classes = db.get("saved_classes", [])
            if class_name not in classes:
                classes.append(class_name)
                db["saved_classes"] = classes
    return await asyncio.to_thread(_write_class)


async def save_and_get_session_history(new_session: dict):
    """Appends current session to SQLite history and returns all past sessions."""
    def _op():
        with SqliteDict(DB_FILE, autocommit=True) as db:
            sessions = db.get("history_sessions", [])
            sessions.append(new_session)
            db["history_sessions"] = sessions
            return sessions
    return await asyncio.to_thread(_op)


# ==================== FLOW 0: REGISTRATION ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    existing = await get_user_record(user_id)

    if existing:
        role = existing.get("flag")
        await update.message.reply_text(
            f"Welcome back, {existing['name']}! You are already registered as a **{role}**.",
            parse_mode="Markdown"
        )
        if role == "teacher":
            await update.message.reply_text("Send /record to start an attendance session.")
        else:
            await update.message.reply_text(
                "You are registered. Enter the attendance OTP when your teacher gives you one.")
        return ConversationHandler.END

    await update.message.reply_text(
        "Welcome to **Minimal Attendance System**\n\nPlease enroll yourself by answering these questions.\n\n"
        "1️⃣ Enter your full name as on your ID card:",
        parse_mode="Markdown"
    )
    return FULL_NAME


async def receive_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data["full_name"] = update.message.text.strip()

    keyboard = [
        [
            InlineKeyboardButton("Teacher 👨‍🏫", callback_data="role_teacher"),
            InlineKeyboardButton("Student 🎓", callback_data="role_student")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text("2️⃣ Select your role:", reply_markup=reply_markup)
    return SELECT_ROLE


async def role_selection(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    role_choice = query.data
    if role_choice == "role_teacher":
        context.user_data["flag"] = "teacher"
        await query.edit_message_text(
            "Role selected: **Teacher**.\n\n🔒 Please enter the teacher registration passcode:", parse_mode="Markdown")
        return TEACHER_PASS
    else:
        context.user_data["flag"] = "student"
        await query.edit_message_text("Role selected: **Student**.\n\n🔢 Enter your roll no.:", parse_mode="Markdown")
        return STUDENT_ROLL


async def verify_teacher_passcode(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    entered_pass = update.message.text.strip()

    if entered_pass == TEACHER_PASSCODE:
        user_data = {
            "user_id": user_id,
            "name": context.user_data["full_name"],
            "flag": "teacher",
            "roll_no": None
        }
        await save_user_record(user_id, user_data)
        await update.message.reply_text(
            "✅ Welcome, Teacher! Registration successful.\n\nUse /record to start hosting attendance sessions.")
        return ConversationHandler.END
    else:
        await update.message.reply_text("❌ Passcode failed, restarting registration process. Send /start to try again.")
        return ConversationHandler.END


async def receive_roll_no(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    roll_no = update.message.text.strip()

    user_data = {
        "user_id": user_id,
        "name": context.user_data["full_name"],
        "flag": "student",
        "roll_no": roll_no
    }
    await save_user_record(user_id, user_data)

    await update.message.reply_text(
        "✅ Registered successfully!\n\nEnter the attendance OTP when the teacher gives you one. "
        "We won't remind you—just record your attendance if given and confirm."
    )
    return ConversationHandler.END


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("Registration cancelled.", reply_markup=ReplyKeyboardRemove())
    return ConversationHandler.END


# ==================== FLOW 1.1: TEACHER ATTENDANCE POSTING ====================
async def record_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_record = await get_user_record(user_id)

    if not user_record or user_record.get("flag") != "teacher":
        await update.message.reply_text("⚠️ Unauthorized. Only registered teachers can use the /record command.")
        return

    if ACTIVE_SESSION["active"]:
        await update.message.reply_text("⚠️ An attendance session is already running right now!")
        return

    saved_subjects = await get_saved_subjects()
    
    keyboard = [[sub] for sub in saved_subjects]
    keyboard.append(["➕ Enter New Subject"])
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)

    context.user_data["teacher_state"] = "GET_SUBJECT"
    await update.message.reply_text(
        "📚 **Select a subject** from your history or choose manual entry:",
        reply_markup=reply_markup,
        parse_mode="Markdown"
    )


async def handle_teacher_inputs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_record = await get_user_record(user_id)
    if not user_record or user_record.get("flag") != "teacher":
        return

    state = context.user_data.get("teacher_state")
    text = update.message.text.strip()

    if state == "GET_SUBJECT":
        if text == "➕ Enter New Subject":
            context.user_data["teacher_state"] = "WAITING_FOR_NEW_SUBJECT"
            await update.message.reply_text(
                "✍️ Please type the name of the new subject:",
                reply_markup=ReplyKeyboardRemove()
            )
        else:
            await add_saved_subject(text)
            context.user_data["subject"] = text
            
            saved_classes = await get_saved_classes()
            class_keyboard = [[c] for c in saved_classes]
            class_keyboard.append(["➕ Enter New Class"])
            class_markup = ReplyKeyboardMarkup(class_keyboard, one_time_keyboard=True, resize_keyboard=True)

            context.user_data["teacher_state"] = "GET_CLASS"
            await update.message.reply_text(
                f"Selected Subject: **{text}**\n\n🏫 **Select a class & year** from history or choose manual entry:",
                reply_markup=class_markup,
                parse_mode="Markdown"
            )

    elif state == "WAITING_FOR_NEW_SUBJECT":
        new_subject = text
        await add_saved_subject(new_subject)
        context.user_data["subject"] = new_subject
        
        saved_classes = await get_saved_classes()
        class_keyboard = [[c] for c in saved_classes]
        class_keyboard.append(["➕ Enter New Class"])
        class_markup = ReplyKeyboardMarkup(class_keyboard, one_time_keyboard=True, resize_keyboard=True)

        context.user_data["teacher_state"] = "GET_CLASS"
        await update.message.reply_text(
            f"✅ Saved & Selected Subject: **{new_subject}**\n\n🏫 **Select a class & year** from history or choose manual entry:",
            reply_markup=class_markup,
            parse_mode="Markdown"
        )

    elif state == "GET_CLASS":
        if text == "➕ Enter New Class":
            context.user_data["teacher_state"] = "WAITING_FOR_NEW_CLASS"
            await update.message.reply_text(
                "✍️ Please type the name of the new class and year (e.g., CSE-3rd Year):",
                reply_markup=ReplyKeyboardRemove()
            )
        else:
            await add_saved_class(text)
            context.user_data["class_info"] = text
            context.user_data["teacher_state"] = None

            keyboard = [[InlineKeyboardButton("🚀 GET OTP", callback_data="generate_otp")]]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await update.message.reply_text(
                f"Subject: **{context.user_data['subject']}**\n"
                f"Class: **{context.user_data['class_info']}**\n\n"
                "Prepare your students, you will get a 5-second expiry OTP. Click below when ready:",
                reply_markup=reply_markup,
                parse_mode="Markdown"
            )

    elif state == "WAITING_FOR_NEW_CLASS":
        new_class = text
        await add_saved_class(new_class)
        context.user_data["class_info"] = new_class
        context.user_data["teacher_state"] = None

        keyboard = [[InlineKeyboardButton("🚀 GET OTP", callback_data="generate_otp")]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await update.message.reply_text(
            f"Subject: **{context.user_data['subject']}**\n"
            f"Class: **{context.user_data['class_info']}**\n\n"
            "Prepare your students, you will get a 5-second expiry OTP. Click below when ready:",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )


async def trigger_otp_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if ACTIVE_SESSION["active"]:
        await query.edit_message_text("⚠️ Session already active.")
        return

    otp = "".join(random.choices(string.digits, k=6))

    ACTIVE_SESSION["active"] = True
    ACTIVE_SESSION["otp"] = otp
    ACTIVE_SESSION["subject"] = context.user_data.get("subject", "General")
    ACTIVE_SESSION["class_info"] = context.user_data.get("class_info", "N/A")
    ACTIVE_SESSION["teacher_id"] = update.effective_user.id
    ACTIVE_SESSION["present_students"] = set()

    await query.edit_message_text(
        f"🔑 **ATTENDANCE OTP GENERATED:** `{otp}`\n\n"
        "Announce this code to your students immediately! It expires in **10 seconds**.",
        parse_mode="Markdown"
    )

    asyncio.create_task(close_attendance_session(context, query.message.chat_id))


async def close_attendance_session(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    # Wait for collection window (10 seconds)
    await asyncio.sleep(10)

    if not ACTIVE_SESSION["active"]:
        return

    ACTIVE_SESSION["active"] = False

    all_students = await get_all_students()
    present_ids = {int(uid) for uid in ACTIVE_SESSION["present_students"]}

    present_count = len(present_ids)
    total_students = len(all_students)
    absent_count = max(0, total_students - present_count)

    current_subject = ACTIVE_SESSION["subject"]
    current_class = ACTIVE_SESSION["class_info"]

    date_label = datetime.now().strftime("Day_%b_%d_%H%M")
    current_session_record = {
        "date_label": date_label,
        "subject": current_subject,
        "class_info": current_class,
        "present_ids": list(present_ids)
    }

    all_sessions = await save_and_get_session_history(current_session_record)

    # Filter history sessions specifically for THIS subject and class combination
    filtered_sessions = [
        s for s in all_sessions 
        if s.get("subject") == current_subject and s.get("class_info") == current_class
    ]

    # Build Subject-Year Wise Cumulative CSV Report in-memory
    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)

    headers = ["Sr.", "Roll no", "Name"] + [s["date_label"] for s in filtered_sessions]
    writer.writerow(headers)

    for idx, student in enumerate(all_students, start=1):
        row = [idx, student["roll_no"], student["name"]]
        for session in filtered_sessions:
            is_present = student["user_id"] in session["present_ids"]
            row.append("P" if is_present else "A")
        writer.writerow(row)

    csv_buffer.seek(0)
    csv_file_bytes = io.BytesIO(csv_buffer.getvalue().encode("utf-8"))
    csv_file_bytes.name = f"attendance_{current_subject}_{current_class}_{datetime.now().strftime('%H%M%S')}.csv"

    await context.bot.send_message(
        chat_id=chat_id,
        text=(
            f"📊 **Attendance Session Closed**\n"
            f"📚 Subject: {current_subject}\n"
            f"🏫 Class: {current_class}\n\n"
            f"✅ Present: {present_count}\n"
            f"❌ Absent: {absent_count}\n\n"
            f"📁 *Subject-Year Cumulative CSV attached below.*"
        ),
        parse_mode="Markdown"
    )

    await context.bot.send_document(chat_id=chat_id, document=csv_file_bytes)


# ==================== FLOW 1.2: STUDENT ATTENDANCE CAPTURE ====================
async def handle_student_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_record = await get_user_record(user_id)

    if not user_record:
        return

    if user_record.get("flag") == "teacher":
        if context.user_data.get("teacher_state"):
            await handle_teacher_inputs(update, context)
        return

    text = update.message.text.strip().upper()

    if not ACTIVE_SESSION["active"]:
        return

    if text == ACTIVE_SESSION["otp"]:
        if int(user_id) in ACTIVE_SESSION["present_students"]:
            await update.message.reply_text("ℹ️ Your attendance was already recorded for this session.")
        else:
            ACTIVE_SESSION["present_students"].add(int(user_id))
            await update.message.reply_text("✅ Response recorded: Present marked successfully!")
    else:
        await update.message.reply_text(
            "❌ Wrong OTP or no call in session. Currently no attendance is being marked or OTP mismatched.")


# ==================== MAIN APPLICATION SETUP ====================
def main():
    application = Application.builder().token("-KEY-").build()

    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            FULL_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_name)],
            SELECT_ROLE: [CallbackQueryHandler(role_selection, pattern="^role_")],
            TEACHER_PASS: [MessageHandler(filters.TEXT & ~filters.COMMAND, verify_teacher_passcode)],
            STUDENT_ROLL: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_roll_no)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    application.add_handler(conv_handler)
    application.add_handler(CommandHandler("record", record_command))
    application.add_handler(CallbackQueryHandler(trigger_otp_callback, pattern="^generate_otp$"))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_student_message))

    loop = asyncio.get_event_loop()
    loop.create_task(db_writer_worker())

    print("🤖 Subject-Year Wise Cumulative Attendance Bot is up and running...")
    application.run_polling()


if __name__ == "__main__":
    main()
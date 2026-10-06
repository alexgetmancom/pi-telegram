/**
 * Public Telegram delivery API
 * Zones: package boundary, telegram delivery, extension interop
 * Exposes target-aware operational view delivery while keeping transport and runtime binding internals package-private
 */
export { deleteTelegramView, editTelegramView, registerTelegramDeliveryTarget, sendTelegramChatAction, sendTelegramView, sendTelegramPhoto, } from "../lib/delivery.js";

import Link from "next/link";

export default function Navbar() {
  return (
    <nav className="bg-gray-800 text-white fixed top-0 left-0 w-full shadow-md z-50 font-[family-name:var(--font-raleway)]">
      <div className="container mx-auto px-4 flex items-center justify-center h-16">
        {/* Navbar Items */}
        <div className="flex space-x-8 text-lg font-semibold">
          <Link href="/" className="hover:text-gray-400">
            Home
          </Link>
          <Link href="/teams" className="hover:text-gray-400">
            Teams
          </Link>
          <Link href="/bdor" className="hover:text-gray-400">
            BDor
          </Link>
          <Link href="/contact" className="hover:text-gray-400">
            Contact
          </Link>
        </div>
      </div>
    </nav>
  );
}

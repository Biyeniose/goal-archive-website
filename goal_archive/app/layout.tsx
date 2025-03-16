import type { Metadata } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import { Roboto, Raleway } from "next/font/google";
//import { Ubuntu_Mono, Ubuntu_Sans_Mono } from "next/font/google";
import {
  Source_Code_Pro,
  Ubuntu,
  Nunito,
  Lato,
  Open_Sans,
  Montserrat,
} from "next/font/google";

import "./globals.css";
import Navbar from "@/components/Navbar";
import InitColorSchemeScript from "@mui/joy/InitColorSchemeScript";
import { CssVarsProvider } from "@mui/joy/styles";
import CssBaseline from "@mui/joy/CssBaseline";

const geistSans = Geist({
  variable: "--font-geist-sans",
  subsets: ["latin"],
});

const montserrat = Montserrat({
  variable: "--font-montserrat",
  subsets: ["latin"],
});

const openSans = Open_Sans({
  variable: "--font-open-sans",
  subsets: ["latin"],
});

const nunito = Nunito({
  variable: "--font-nunito",
  subsets: ["latin"],
});

const lato = Lato({
  variable: "--font-lato",
  subsets: ["latin"],
  weight: "100",
});

const ubuntu = Ubuntu({
  variable: "--font-ubuntu",
  subsets: ["latin"],
  weight: "300",
});

const sourceCodePro = Source_Code_Pro({
  variable: "--font-source-code-pro",
  subsets: ["latin"],
});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

const roboto = Roboto({
  weight: ["500"], // Choose weights: 400 (regular), 700 (bold), etc.
  subsets: ["latin"],
  variable: "--font-roboto", // Optional CSS variable for custom font
});

const raleway = Raleway({
  weight: ["700"],
  subsets: ["latin"],
  variable: "--font-raleway",
});

export const metadata: Metadata = {
  title: "Goal Archive",
  description: "Explore Footall Stats",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" suppressHydrationWarning={true}>
      <body
        className={`${geistSans.variable} ${geistMono.variable} ${roboto.variable} ${montserrat.variable} ${openSans.variable} ${lato.variable} ${nunito.variable} ${ubuntu.variable} ${sourceCodePro.variable} ${raleway.variable} antialiased`}
      >
        <InitColorSchemeScript />
        <CssVarsProvider>
          <CssBaseline />
          <Navbar />
          {children}
        </CssVarsProvider>
      </body>
    </html>
  );
}
